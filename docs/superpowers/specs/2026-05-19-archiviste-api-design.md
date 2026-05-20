# Archiviste v1 — API design

Date: 2026-05-19

## Purpose

A streaming-first Elixir library for reading WARC (Web ARChive, ISO 28500) files.
The existing Elixir ecosystem has CDX-focused tooling
([`common_crawl`](https://github.com/preciz/common_crawl)) but lacks a proper
WARC parser; Archiviste fills that gap.

## Scope

### In (v1)

- Reading WARC 1.0 and 1.1
- Plain (`.warc`) and per-record gzipped (`.warc.gz`) files
- Streaming-first API built on `Stream.t()`
- Two layered APIs:
  - **Low-level** — yields `%Archiviste.Record{}` with raw payload as a lazy
    stream of binary chunks
  - **High-level HTTP** — for `response` / `request` records, parses the
    embedded HTTP message into structs with status line, headers, and a lazy
    body stream
- Lenient parsing by default; strict mode available per call
- Random-access read by file offset (`Archiviste.read_at!/3`)
- Opt-in WARC digest verification (`WARC-Block-Digest` /
  `WARC-Payload-Digest`)
- Opt-in HTTP body `Content-Encoding` decode (gzip / deflate / brotli / zstd)

### Out (v1, but additive later)

- Writing WARC files
- CDX index parsing (defer to existing `common_crawl` library or a future
  `Archiviste.CDX` module)
- Legacy ARC format
- Sharded / multi-file indexes
- Async/parallel decode (the streaming API composes fine with
  `Task.async_stream/3` upstream — no built-in concurrency needed)

## Module layout

```
Archiviste                  -- entry points (stream!, stream_file!, read_at!)
Archiviste.Record           -- %Record{} struct + payload helpers
Archiviste.HTTP             -- HTTP-layer parsing
  Archiviste.HTTP.Request   -- %Request{} struct
  Archiviste.HTTP.Response  -- %Response{} struct
Archiviste.Error            -- exception types
Archiviste.Parser           -- (internal) binary state machine
Archiviste.Gzip             -- (internal) per-record gunzip
Archiviste.Digest           -- (internal) digest verification
```

## Public API

### Reading

```elixir
@spec stream!(Enumerable.t(binary()), keyword()) :: Enumerable.t(Record.t())
Archiviste.stream!(enumerable_of_binaries, opts \\ [])

@spec stream_file!(Path.t(), keyword()) :: Enumerable.t(Record.t())
Archiviste.stream_file!(path, opts \\ [])

@spec read_at!(Path.t(), non_neg_integer(), keyword()) :: Record.t()
Archiviste.read_at!(path, offset, opts \\ [])
```

`stream_file!/2` is a thin convenience over `File.stream!/3 |> stream!/2` that
also auto-detects gzip from the `.gz` extension and/or the first two bytes
(`1f 8b`). Splitting these gives clean types: `stream!/2` is the pure core,
`stream_file!/2` is the I/O-coupled helper.

### HTTP layer

```elixir
@spec parse(Record.t(), keyword()) ::
  {:ok, %HTTP.Response{} | %HTTP.Request{}} | {:error, term()}
Archiviste.HTTP.parse(record, opts \\ [])

@spec parse_stream(keyword()) ::
  (Enumerable.t(Record.t()) -> Enumerable.t(Record.t() | %HTTP.Response{} | %HTTP.Request{}))
Archiviste.HTTP.parse_stream(opts \\ [])
```

`parse/2` is the testable primitive. `parse_stream/1` is a stream stage that
replaces `response` / `request` records' raw payloads with parsed HTTP structs
in place, and leaves other record types untouched.

### Common opts

| Option            | Default | Applies to                  | Effect                                                                                                                              |
| ----------------- | ------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `:strict`         | `false` | `stream!`, `stream_file!`   | When `true`, any malformed-data error raises mid-stream instead of being skipped with a `Logger.warning`.                           |
| `:verify_digests` | `false` | `stream!`, `stream_file!`   | When `true`, verify `WARC-Block-Digest` / `WARC-Payload-Digest`. Mismatch is treated as a malformed-record error (see lenient rules). |
| `:decode_body`    | `false` | `HTTP.parse`, `parse_stream` | When `true`, transparently decode HTTP `Content-Encoding`. Missing optional NIF dep for a present encoding **raises** unconditionally — this is a programmer error, not bad data. |

## `%Archiviste.Record{}`

```elixir
defmodule Archiviste.Record do
  @type warc_type ::
    :warcinfo | :response | :request | :metadata
    | :resource | :revisit | :conversion | :continuation
    | binary()  # unknown / future types preserved verbatim

  @type t :: %__MODULE__{
    version: String.t(),              # "WARC/1.1"
    type: warc_type(),
    id: String.t(),                   # "<urn:uuid:...>"
    date: DateTime.t(),
    target_uri: String.t() | nil,     # nil for warcinfo
    content_type: String.t() | nil,
    content_length: non_neg_integer(),
    headers: %{String.t() => String.t()},  # all WARC headers verbatim, lowercased keys
    payload: Enumerable.t(binary()),  # lazy, single-pass — see "Payload handle"
    offset: non_neg_integer()         # byte offset in source
  }
end
```

Helpers:

```elixir
Archiviste.Record.read_payload(record)     # eager binary; use only when payloads are small
Archiviste.Record.discard_payload(record)  # drain and ignore
```

## Payload handle (the crucial part)

`record.payload` is a `Stream.t()` of binary chunks with these invariants:

- **Forward-only, single-pass.** Reading consumes it; re-reading raises.
- **Auto-discarded.** If the outer stream advances to the next record while
  `payload` hasn't been fully consumed, the parser silently drains the
  remaining bytes:
  - plain files — `:file.position/2` forward by the remaining length
  - gzipped — run the gzip decoder to the end of the current member
- The same shape is used for `%HTTP.Response{body: <stream>}` and
  `%HTTP.Request{body: <stream>}` so memory behavior is consistent across
  layers.

This means filtering on headers is free:

```elixir
"crawl.warc.gz"
|> Archiviste.stream_file!()
|> Stream.filter(&(&1.type == :response))      # never touches the payload
|> ...
```

## `%Archiviste.HTTP.Response{}` / `Request{}`

```elixir
defmodule Archiviste.HTTP.Response do
  @type t :: %__MODULE__{
    record: Archiviste.Record.t(),       # underlying WARC record
    status: 100..599,
    reason: String.t(),                  # "OK"
    http_version: String.t(),            # "HTTP/1.1"
    headers: [{String.t(), String.t()}], # list of pairs (HTTP allows duplicates)
    body: Enumerable.t(binary()),        # lazy, same contract as Record.payload
    body_encoding: nil | :gzip | :deflate | :br | :zstd | :identity | binary()
  }
end
```

`Request` is analogous (method + path + version + headers + body).

When `decode_body: true`:
- `body` yields decoded bytes
- `body_encoding` retains the original encoding name for caller awareness

When `decode_body: false` (default):
- `body` yields the raw bytes as captured on the wire
- `body_encoding` still reports the announced `Content-Encoding`

## Compression: WARC layer vs HTTP layer

Two independent compression layers, handled differently:

**1. WARC-level compression** — the spec only defines per-record **gzip**.
Archiviste supports this via stdlib `:zlib`. No other WARC-level encodings.
(Unofficial `.warc.zst` is out of scope; if it becomes standardized, it's
additive.)

**2. HTTP body `Content-Encoding`** — opt-in via `decode_body: true`:

| Encoding   | Decoder           | Required dep      |
| ---------- | ----------------- | ----------------- |
| `gzip`     | `:zlib` (stdlib)  | none              |
| `deflate`  | `:zlib` (stdlib)  | none              |
| `br`       | `:brotli` NIF     | `{:brotli, ...}`  |
| `zstd`     | `:ezstd` NIF      | `{:ezstd, ...}`   |
| `identity` | passthrough       | none              |

Brotli and zstd are declared as **optional** deps in `mix.exs`. At runtime, if
`decode_body: true` and a record uses an encoding whose decoder isn't loaded,
the library raises `Archiviste.Error.UnsupportedEncodingError` with a message
that names the encoding and the dep to add. This raise is **unconditional**
— it does not depend on `:strict`, because a missing dep is a programmer
error, not malformed data.

## Error handling

Errors split into two categories:

### Malformed data (subject to lenient/strict toggle)

- Bad WARC header line
- `Content-Length` mismatch / truncation mid-payload
- Gzip CRC failure
- Digest mismatch (when verification is on)
- Truncated file (EOF mid-record)

**Lenient (default):**
- `Logger.warning("Archiviste: skipped malformed record at offset N: <reason>")`
- The stream continues with the next record (or terminates cleanly on EOF)

**Strict (`strict: true`):**
- Raises `Archiviste.Error.MalformedRecordError`,
  `Archiviste.Error.DigestMismatchError`, or
  `Archiviste.Error.TruncatedFileError` mid-stream

### Programmer errors (always raise)

- `Archiviste.Error.UnsupportedEncodingError` — body decode requested,
  required NIF dep not loaded
- `ArgumentError` — invalid opts, bad paths, etc.

## Composition example

```elixir
"crawl.warc.gz"
|> Archiviste.stream_file!()
|> Stream.filter(&(&1.type == :response))
|> Archiviste.HTTP.parse_stream(decode_body: true)
|> Stream.filter(&match?(%Archiviste.HTTP.Response{status: 200}, &1))
|> Stream.map(&extract_title/1)
|> Enum.to_list()
```

## Testing strategy

1. **Handcrafted fixture WARCs** in `test/support/fixtures/`:
   - Minimal valid single-record file
   - Multi-record file with each `WARC-Type`
   - Gzipped variant of each
   - Malformed cases: bad `Content-Length`, truncated payload, garbage between
     records, mixed line endings, missing required headers
2. **Property-based parser tests** with `StreamData`: generate well-formed
   record bytes, round-trip-parse them, assert structural equality and
   exact-byte payload preservation.
3. **Real-world torture fixture** — a small (few KB) extract from a public
   Common Crawl sample, gated behind `@tag :slow` so CI can opt in/out.
4. **HTTP layer** tested independently against fixture payload bytes (no need
   to involve the WARC parser).
5. **Lenient/strict matrix** — every malformed fixture is tested under both
   modes, asserting the expected log message vs. the expected raise.

## Non-functional notes

- **Memory:** processing a 10 GB WARC must use bounded memory regardless of
  individual record size. The payload-handle contract is the load-bearing
  piece here.
- **Throughput:** target is "saturate one core's gzip decompression rate" on
  the streaming path. No premature concurrency in the library itself; callers
  parallelize across files with `Task.async_stream/3`.
- **Dependencies:** core lib has zero runtime deps (only stdlib). Brotli and
  zstd are optional. ExDoc / Credo / Dialyxir / ExCoveralls are dev/test only.

## Open questions (deferred, do not block v1)

- Whether to expose a public `Archiviste.Parser.parse_chunk/2` for users who
  want to drive the state machine directly (e.g., wrapping in a `GenStage`).
  Probably yes eventually, but not in v1's public API.
- Whether to ship a `Mix.Tasks.Warc.Inspect` CLI for quick file inspection.
  Nice-to-have, defer.
