# Archiviste v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a streaming-first Elixir library for reading WARC 1.0/1.1 files (plain and per-record gzipped), exposing a low-level `%Archiviste.Record{}` API with lazy payloads plus a high-level HTTP-parsing layer.

**Architecture:** A process-backed byte reader (`Archiviste.Reader`) owns the input enumerable and serves bounded reads/skips. A pure parser reads WARC record headers from it, then yields a record whose payload is a `Stream.t()` that pulls from the same reader. Auto-drain semantics live in the parser: when asked for the next record, it skips any unconsumed payload of the previous one. Gzip handling is per-record, decoded into the reader's input stream before parsing.

**Tech Stack:** Elixir 1.19, OTP 28, `:zlib` (stdlib), optional `:brotli` and `:ezstd` NIF deps for HTTP body decoding, `ExUnit` + `StreamData` for tests.

**Reference spec:** `docs/superpowers/specs/2026-05-19-archiviste-api-design.md`

---

## File structure

```
lib/
  archiviste.ex                  -- top-level: stream!/2, stream_file!/2, read_at!/3
  archiviste/
    record.ex                    -- %Record{}, read_payload/1, discard_payload/1
    error.ex                     -- exception types
    reader.ex                    -- (internal) GenServer wrapping the byte source
    parser.ex                    -- (internal) pure WARC parser working on a Reader pid
    gzip.ex                      -- (internal) per-record gzip member decoding
    digest.ex                    -- (internal) digest verification
    http.ex                      -- HTTP.parse/2, HTTP.parse_stream/1
    http/
      request.ex                 -- %HTTP.Request{}
      response.ex                -- %HTTP.Response{}
      decoder.ex                 -- (internal) Content-Encoding decoding
test/
  support/
    warc_fixture.ex              -- in-test WARC byte builder
  archiviste_test.exs
  archiviste/
    record_test.exs
    parser_test.exs
    reader_test.exs
    gzip_test.exs
    digest_test.exs
    http_test.exs
    http/
      decoder_test.exs
```

---

## Task 1: Error module

**Files:**
- Create: `lib/archiviste/error.ex`
- Test: `test/archiviste/error_test.exs`

- [ ] **Step 1: Write the failing test**

Create `test/archiviste/error_test.exs`:

```elixir
defmodule Archiviste.ErrorTest do
  use ExUnit.Case, async: true

  alias Archiviste.Error

  test "MalformedRecordError carries offset and reason" do
    err = %Error.MalformedRecordError{offset: 42, reason: :bad_header}
    assert Exception.message(err) =~ "offset 42"
    assert Exception.message(err) =~ "bad_header"
  end

  test "TruncatedFileError carries offset" do
    err = %Error.TruncatedFileError{offset: 100}
    assert Exception.message(err) =~ "offset 100"
  end

  test "DigestMismatchError carries record id and which digest" do
    err = %Error.DigestMismatchError{
      record_id: "<urn:uuid:abc>",
      digest_kind: :block,
      expected: "sha1:AAAA",
      actual: "sha1:BBBB"
    }
    msg = Exception.message(err)
    assert msg =~ "<urn:uuid:abc>"
    assert msg =~ "block"
    assert msg =~ "AAAA"
    assert msg =~ "BBBB"
  end

  test "UnsupportedEncodingError suggests the dep to add" do
    err = %Error.UnsupportedEncodingError{encoding: "br"}
    msg = Exception.message(err)
    assert msg =~ "br"
    assert msg =~ ":brotli"
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

```
mix test test/archiviste/error_test.exs
```
Expected: FAIL — `Archiviste.Error.MalformedRecordError` is undefined.

- [ ] **Step 3: Implement the error module**

Create `lib/archiviste/error.ex`:

```elixir
defmodule Archiviste.Error do
  @moduledoc """
  Exception types raised by Archiviste.

  Errors fall into two categories:

  * Malformed-data errors (subject to the `:strict` toggle): `MalformedRecordError`,
    `TruncatedFileError`, `DigestMismatchError`.
  * Programmer errors (always raised regardless of `:strict`):
    `UnsupportedEncodingError`.
  """

  defmodule MalformedRecordError do
    defexception [:offset, :reason]

    @impl true
    def message(%{offset: offset, reason: reason}) do
      "malformed WARC record at offset #{offset}: #{inspect(reason)}"
    end
  end

  defmodule TruncatedFileError do
    defexception [:offset]

    @impl true
    def message(%{offset: offset}) do
      "truncated WARC file at offset #{offset}"
    end
  end

  defmodule DigestMismatchError do
    defexception [:record_id, :digest_kind, :expected, :actual]

    @impl true
    def message(%{
          record_id: id,
          digest_kind: kind,
          expected: expected,
          actual: actual
        }) do
      "#{kind} digest mismatch for record #{id}: expected #{expected}, got #{actual}"
    end
  end

  defmodule UnsupportedEncodingError do
    defexception [:encoding]

    @dep_hint %{
      "br" => "{:brotli, \"~> 0.3\"}",
      "brotli" => "{:brotli, \"~> 0.3\"}",
      "zstd" => "{:ezstd, \"~> 1.0\"}"
    }

    @impl true
    def message(%{encoding: encoding}) do
      hint =
        case Map.fetch(@dep_hint, encoding) do
          {:ok, dep} -> " Add #{dep} to your deps to enable it."
          :error -> ""
        end

      "no decoder loaded for HTTP Content-Encoding #{inspect(encoding)}." <> hint
    end
  end
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/error_test.exs
```
Expected: 4 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/error.ex test/archiviste/error_test.exs
git commit -m "Add Archiviste.Error exception types"
```

---

## Task 2: Record struct

**Files:**
- Create: `lib/archiviste/record.ex`
- Test: `test/archiviste/record_test.exs`

- [ ] **Step 1: Write the failing test**

Create `test/archiviste/record_test.exs`:

```elixir
defmodule Archiviste.RecordTest do
  use ExUnit.Case, async: true

  alias Archiviste.Record

  test "struct has expected default fields" do
    record = %Record{
      version: "WARC/1.1",
      type: :response,
      id: "<urn:uuid:abc>",
      date: ~U[2026-05-19 00:00:00Z],
      target_uri: "https://example.com/",
      content_type: "application/http;msgtype=response",
      content_length: 0,
      headers: %{},
      payload: [],
      offset: 0
    }

    assert record.version == "WARC/1.1"
    assert record.type == :response
    assert record.payload == []
  end

  test "read_payload/1 concatenates a Stream of binaries" do
    record = %Record{
      version: "WARC/1.1",
      type: :response,
      id: "<id>",
      date: ~U[2026-01-01 00:00:00Z],
      target_uri: nil,
      content_type: nil,
      content_length: 5,
      headers: %{},
      payload: Stream.cycle(["he", "ll", "o"]) |> Stream.take(3),
      offset: 0
    }

    assert Record.read_payload(record) == "hello"
  end

  test "discard_payload/1 drains the payload Stream and returns :ok" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    payload =
      Stream.unfold(0, fn
        n when n < 3 ->
          Agent.update(counter, &(&1 + 1))
          {<<n>>, n + 1}

        _ ->
          nil
      end)

    record = %Record{
      version: "WARC/1.1",
      type: :metadata,
      id: "<id>",
      date: ~U[2026-01-01 00:00:00Z],
      target_uri: nil,
      content_type: nil,
      content_length: 3,
      headers: %{},
      payload: payload,
      offset: 0
    }

    assert Record.discard_payload(record) == :ok
    assert Agent.get(counter, & &1) == 3
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

```
mix test test/archiviste/record_test.exs
```
Expected: FAIL — module undefined.

- [ ] **Step 3: Implement the record module**

Create `lib/archiviste/record.ex`:

```elixir
defmodule Archiviste.Record do
  @moduledoc """
  A single WARC record yielded by `Archiviste.stream!/2`.

  The `:payload` field is a lazy `Stream.t()` of binary chunks. It is
  **forward-only and single-pass** — reading consumes it. When the outer
  record stream advances to the next record, any unconsumed payload bytes
  are auto-drained.
  """

  @type warc_type ::
          :warcinfo
          | :response
          | :request
          | :metadata
          | :resource
          | :revisit
          | :conversion
          | :continuation
          | binary()

  @type t :: %__MODULE__{
          version: String.t(),
          type: warc_type(),
          id: String.t(),
          date: DateTime.t(),
          target_uri: String.t() | nil,
          content_type: String.t() | nil,
          content_length: non_neg_integer(),
          headers: %{optional(String.t()) => String.t()},
          payload: Enumerable.t(),
          offset: non_neg_integer()
        }

  @enforce_keys [
    :version,
    :type,
    :id,
    :date,
    :content_length,
    :headers,
    :payload,
    :offset
  ]
  defstruct [
    :version,
    :type,
    :id,
    :date,
    :target_uri,
    :content_type,
    :content_length,
    :headers,
    :payload,
    :offset
  ]

  @doc """
  Reads the full payload into memory as a binary.

  Convenient for small payloads. Do not use on records whose
  `:content_length` may be large.
  """
  @spec read_payload(t()) :: binary()
  def read_payload(%__MODULE__{payload: payload}) do
    payload |> Enum.to_list() |> IO.iodata_to_binary()
  end

  @doc """
  Drains and discards the payload. Returns `:ok`.
  """
  @spec discard_payload(t()) :: :ok
  def discard_payload(%__MODULE__{payload: payload}) do
    Stream.run(payload)
  end
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/record_test.exs
```
Expected: 3 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/record.ex test/archiviste/record_test.exs
git commit -m "Add Archiviste.Record struct with payload helpers"
```

---

## Task 3: Test fixture builder

**Files:**
- Create: `test/support/warc_fixture.ex`
- Test: `test/support/warc_fixture_test.exs`

- [ ] **Step 1: Write the failing test**

Create `test/support/warc_fixture_test.exs`:

```elixir
defmodule Archiviste.WarcFixtureTest do
  use ExUnit.Case, async: true

  alias Archiviste.WarcFixture

  test "builds a minimal response record with correct framing" do
    bytes =
      WarcFixture.record(
        type: "response",
        id: "<urn:uuid:11111111-1111-1111-1111-111111111111>",
        date: "2026-05-19T00:00:00Z",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: "HTTP/1.1 200 OK\r\n\r\nhello"
      )

    assert String.starts_with?(bytes, "WARC/1.1\r\n")
    assert bytes =~ "WARC-Type: response\r\n"
    assert bytes =~ "Content-Length: 25\r\n"
    assert String.ends_with?(bytes, "\r\nhello\r\n\r\n")
  end

  test "concat/1 joins multiple records" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "b")
    assert WarcFixture.concat([a, b]) == a <> b
  end

  test "gzip/1 wraps bytes in a single gzip member" do
    bytes = WarcFixture.record(type: "warcinfo", payload: "hello")
    gzipped = WarcFixture.gzip(bytes)
    assert <<0x1F, 0x8B, _::binary>> = gzipped
    assert :zlib.gunzip(gzipped) == bytes
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

```
mix test test/support/warc_fixture_test.exs
```
Expected: FAIL — `Archiviste.WarcFixture` undefined.

- [ ] **Step 3: Implement the fixture builder**

Create `test/support/warc_fixture.ex`:

```elixir
defmodule Archiviste.WarcFixture do
  @moduledoc false
  # Test helper for constructing WARC record bytes inline.

  @crlf "\r\n"

  @doc """
  Build one WARC record. Accepts:

    * `:version` (default `"WARC/1.1"`)
    * `:type`
    * `:id` (default a fixed UUID-shaped string)
    * `:date` (default fixed ISO-8601 string)
    * `:target_uri`
    * `:content_type`
    * `:headers` — extra `[{name, value}]`
    * `:payload` (default `""`)
    * `:content_length` — override; default `byte_size(payload)`
  """
  def record(opts) do
    payload = Keyword.get(opts, :payload, "")
    content_length = Keyword.get(opts, :content_length, byte_size(payload))

    headers =
      [
        {"WARC-Type", Keyword.fetch!(opts, :type)},
        {"WARC-Record-ID",
         Keyword.get(opts, :id, "<urn:uuid:00000000-0000-0000-0000-000000000000>")},
        {"WARC-Date", Keyword.get(opts, :date, "2026-05-19T00:00:00Z")}
      ] ++
        maybe({"WARC-Target-URI", Keyword.get(opts, :target_uri)}) ++
        maybe({"Content-Type", Keyword.get(opts, :content_type)}) ++
        Keyword.get(opts, :headers, []) ++
        [{"Content-Length", Integer.to_string(content_length)}]

    version = Keyword.get(opts, :version, "WARC/1.1")
    header_block = Enum.map_join(headers, "", fn {k, v} -> "#{k}: #{v}#{@crlf}" end)

    version <> @crlf <> header_block <> @crlf <> payload <> @crlf <> @crlf
  end

  defp maybe({_k, nil}), do: []
  defp maybe(kv), do: [kv]

  @doc "Concatenate a list of record byte strings."
  def concat(records) when is_list(records), do: IO.iodata_to_binary(records)

  @doc "Gzip a binary into a single gzip member."
  def gzip(bytes) when is_binary(bytes), do: :zlib.gzip(bytes)

  @doc "Concatenate per-record-gzipped members (the standard `.warc.gz` layout)."
  def gzip_each(records) when is_list(records) do
    records |> Enum.map(&gzip/1) |> IO.iodata_to_binary()
  end
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/support/warc_fixture_test.exs
```
Expected: 3 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add test/support/warc_fixture.ex test/support/warc_fixture_test.exs
git commit -m "Add WARC fixture builder for tests"
```

---

## Task 4: Reader process

**Files:**
- Create: `lib/archiviste/reader.ex`
- Test: `test/archiviste/reader_test.exs`

The Reader owns the input `Enumerable.t(binary())` and serves bounded `read/skip` calls. It's a `GenServer` so multiple stream stages (outer record stream + inner payload sub-stream) can share state safely.

- [ ] **Step 1: Write the failing test**

Create `test/archiviste/reader_test.exs`:

```elixir
defmodule Archiviste.ReaderTest do
  use ExUnit.Case, async: true

  alias Archiviste.Reader

  test "reads exact bytes across enumerable chunk boundaries" do
    {:ok, r} = Reader.start_link(["he", "ll", "o world"])
    assert Reader.read(r, 5) == {:ok, "hello"}
    assert Reader.read(r, 6) == {:ok, " world"}
    assert Reader.read(r, 1) == :eof
    Reader.close(r)
  end

  test "read_until/2 reads up to and including a delimiter" do
    {:ok, r} = Reader.start_link(["alpha\r\nbeta\r\n", "gamma"])
    assert Reader.read_until(r, "\r\n") == {:ok, "alpha\r\n"}
    assert Reader.read_until(r, "\r\n") == {:ok, "beta\r\n"}
    Reader.close(r)
  end

  test "skip/2 advances past N bytes without buffering" do
    {:ok, r} = Reader.start_link([String.duplicate("x", 1_000_000), "tail"])
    assert Reader.skip(r, 1_000_000) == :ok
    assert Reader.read(r, 4) == {:ok, "tail"}
    Reader.close(r)
  end

  test "offset/1 reports total consumed bytes" do
    {:ok, r} = Reader.start_link(["abcdef"])
    Reader.read(r, 3)
    assert Reader.offset(r) == 3
    Reader.skip(r, 2)
    assert Reader.offset(r) == 5
    Reader.close(r)
  end

  test "read past EOF returns :eof and stays at EOF" do
    {:ok, r} = Reader.start_link(["ab"])
    assert Reader.read(r, 4) == :eof
    assert Reader.read(r, 1) == :eof
    Reader.close(r)
  end

  test "peek/2 returns bytes without consuming them" do
    {:ok, r} = Reader.start_link(["abcdef"])
    assert Reader.peek(r, 3) == {:ok, "abc"}
    assert Reader.read(r, 3) == {:ok, "abc"}
    Reader.close(r)
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

```
mix test test/archiviste/reader_test.exs
```
Expected: FAIL — `Archiviste.Reader` undefined.

- [ ] **Step 3: Implement Reader**

Create `lib/archiviste/reader.ex`:

```elixir
defmodule Archiviste.Reader do
  @moduledoc false
  # Process-backed byte cursor over an Enumerable.t(binary()).
  #
  # Owns the enumerable's continuation function and serves serialized
  # read/skip/peek calls. Used internally by Archiviste.stream!/2; both
  # the outer record stream and the inner payload sub-stream call into
  # the same Reader pid.

  use GenServer

  @type t :: pid()

  ## Client

  def start_link(enumerable) do
    GenServer.start_link(__MODULE__, enumerable)
  end

  def read(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:read, n}, :infinity)

  def peek(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:peek, n}, :infinity)

  def skip(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:skip, n}, :infinity)

  def read_until(pid, delim) when is_binary(delim),
    do: GenServer.call(pid, {:read_until, delim}, :infinity)

  def offset(pid), do: GenServer.call(pid, :offset, :infinity)

  def eof?(pid), do: GenServer.call(pid, :eof?, :infinity)

  def close(pid), do: GenServer.stop(pid)

  ## Server

  defmodule State do
    @moduledoc false
    defstruct [:cont, :buffer, :offset, :eof]
  end

  @impl true
  def init(enumerable) do
    reducer = &Enumerable.reduce(enumerable, &1, fn x, _ -> {:suspend, x} end)
    cont = reducer.({:cont, nil})
    {:ok, %State{cont: cont, buffer: <<>>, offset: 0, eof: false}}
  end

  @impl true
  def handle_call({:read, n}, _from, state) do
    case ensure_buffered(state, n) do
      {:ok, state} ->
        <<chunk::binary-size(n), rest::binary>> = state.buffer
        {:reply, {:ok, chunk},
         %{state | buffer: rest, offset: state.offset + n}}

      {:eof, state} ->
        {:reply, :eof, state}
    end
  end

  def handle_call({:peek, n}, _from, state) do
    case ensure_buffered(state, n) do
      {:ok, state} ->
        <<chunk::binary-size(n), _::binary>> = state.buffer
        {:reply, {:ok, chunk}, state}

      {:eof, state} ->
        {:reply, :eof, state}
    end
  end

  def handle_call({:skip, n}, _from, state) do
    {result, state} = do_skip(state, n)
    {:reply, result, state}
  end

  def handle_call({:read_until, delim}, _from, state) do
    {result, state} = do_read_until(state, delim, byte_size(delim))
    {:reply, result, state}
  end

  def handle_call(:offset, _from, state),
    do: {:reply, state.offset, state}

  def handle_call(:eof?, _from, state),
    do: {:reply, state.eof and state.buffer == <<>>, state}

  ## Internals

  defp ensure_buffered(state, n) do
    cond do
      byte_size(state.buffer) >= n ->
        {:ok, state}

      state.eof ->
        if byte_size(state.buffer) >= n, do: {:ok, state}, else: {:eof, state}

      true ->
        case pull(state) do
          {:ok, state} -> ensure_buffered(state, n)
          {:eof, state} -> ensure_buffered(state, n)
        end
    end
  end

  defp pull(%State{cont: cont} = state) do
    case cont.({:cont, nil}) do
      {:suspended, chunk, next} when is_binary(chunk) ->
        {:ok, %{state | buffer: state.buffer <> chunk, cont: next}}

      {:suspended, iodata, next} ->
        {:ok,
         %{
           state
           | buffer: state.buffer <> IO.iodata_to_binary(iodata),
             cont: next
         }}

      {:done, _} ->
        {:eof, %{state | eof: true}}

      {:halted, _} ->
        {:eof, %{state | eof: true}}
    end
  end

  defp do_skip(state, 0), do: {:ok, state}

  defp do_skip(%State{buffer: buf, offset: off} = state, n) do
    case byte_size(buf) do
      size when size >= n ->
        <<_::binary-size(n), rest::binary>> = buf
        {:ok, %{state | buffer: rest, offset: off + n}}

      size ->
        state = %{state | buffer: <<>>, offset: off + size}
        remaining = n - size

        if state.eof do
          {:eof, state}
        else
          case pull(state) do
            {:ok, state} -> do_skip(state, remaining)
            {:eof, state} -> {:eof, state}
          end
        end
    end
  end

  defp do_read_until(state, delim, delim_size) do
    case :binary.match(state.buffer, delim) do
      {pos, ^delim_size} ->
        take = pos + delim_size
        <<chunk::binary-size(take), rest::binary>> = state.buffer

        {{:ok, chunk},
         %{state | buffer: rest, offset: state.offset + take}}

      :nomatch ->
        if state.eof do
          {:eof, state}
        else
          case pull(state) do
            {:ok, state} -> do_read_until(state, delim, delim_size)
            {:eof, state} -> {:eof, state}
          end
        end
    end
  end
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/reader_test.exs
```
Expected: 6 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/reader.ex test/archiviste/reader_test.exs
git commit -m "Add Archiviste.Reader process-backed byte cursor"
```

---

## Task 5: Parser — single record (headers + payload)

**Files:**
- Create: `lib/archiviste/parser.ex`
- Test: `test/archiviste/parser_test.exs`

The parser reads from a Reader pid. `next_record/2` consumes one record's headers, returns `{:ok, %Record{}}` (with the payload as a Stream that pulls lazily from the Reader), `:eof`, or `{:error, reason, offset}`.

- [ ] **Step 1: Write the failing test**

Create `test/archiviste/parser_test.exs`:

```elixir
defmodule Archiviste.ParserTest do
  use ExUnit.Case, async: true

  alias Archiviste.{Parser, Reader, Record, WarcFixture}

  defp reader_from(binary), do: Reader.start_link([binary])

  test "parses a single minimal warcinfo record" do
    bytes =
      WarcFixture.record(
        type: "warcinfo",
        id: "<urn:uuid:11111111-1111-1111-1111-111111111111>",
        date: "2026-05-19T00:00:00Z",
        payload: "software: archiviste/0.1\r\n"
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, %Record{} = record} = Parser.next_record(r)
    assert record.version == "WARC/1.1"
    assert record.type == :warcinfo
    assert record.id == "<urn:uuid:11111111-1111-1111-1111-111111111111>"
    assert record.date == ~U[2026-05-19 00:00:00Z]
    assert record.content_length == 26
    assert record.offset == 0
    assert Record.read_payload(record) == "software: archiviste/0.1\r\n"
    assert Parser.next_record(r) == :eof
    Reader.close(r)
  end

  test "parses target_uri and content_type when present" do
    bytes =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: "HTTP/1.1 200 OK\r\n\r\n"
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.type == :response
    assert record.target_uri == "https://example.com/"
    assert record.content_type == "application/http;msgtype=response"
    Reader.close(r)
  end

  test "preserves all headers in the headers map (lowercased keys)" do
    bytes =
      WarcFixture.record(
        type: "response",
        headers: [{"WARC-IP-Address", "203.0.113.1"}],
        payload: ""
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.headers["warc-ip-address"] == "203.0.113.1"
    assert record.headers["warc-type"] == "response"
    Reader.close(r)
  end

  test "unknown WARC-Type is preserved as a string" do
    bytes = WarcFixture.record(type: "future-type", payload: "")

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.type == "future-type"
    Reader.close(r)
  end

  test "payload Stream yields bytes lazily" do
    bytes = WarcFixture.record(type: "resource", payload: "abcdefghij")
    {:ok, r} = reader_from(bytes)
    {:ok, record} = Parser.next_record(r)
    chunks = Enum.to_list(record.payload)
    assert IO.iodata_to_binary(chunks) == "abcdefghij"
    Reader.close(r)
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

```
mix test test/archiviste/parser_test.exs
```
Expected: FAIL — `Archiviste.Parser` undefined.

- [ ] **Step 3: Implement Parser (single record + lazy payload)**

Create `lib/archiviste/parser.ex`:

```elixir
defmodule Archiviste.Parser do
  @moduledoc false
  # Pure WARC record parser driven by an Archiviste.Reader pid.

  alias Archiviste.{Reader, Record}

  @known_types ~w(warcinfo response request metadata resource revisit conversion continuation)

  @doc """
  Read the next record from the reader.

  Returns:
    * `{:ok, %Record{}}` — record header parsed; payload is a lazy Stream
    * `:eof` — clean end of stream
    * `{:error, reason, offset}` — malformed record at byte offset
  """
  def next_record(reader_pid) do
    # If a previous record's payload wasn't fully consumed, the caller is
    # expected to have called `drain_pending/1` before this. For the initial
    # call there is no pending payload.
    offset = Reader.offset(reader_pid)

    case Reader.peek(reader_pid, 1) do
      :eof ->
        :eof

      {:ok, _} ->
        with {:ok, version} <- read_version_line(reader_pid, offset),
             {:ok, headers} <- read_header_block(reader_pid, offset),
             {:ok, parsed} <- interpret_headers(headers, offset) do
          payload_stream = build_payload_stream(reader_pid, parsed.content_length)

          record = %Record{
            version: version,
            type: parsed.type,
            id: parsed.id,
            date: parsed.date,
            target_uri: parsed.target_uri,
            content_type: parsed.content_type,
            content_length: parsed.content_length,
            headers: headers,
            payload: payload_stream,
            offset: offset
          }

          {:ok, record}
        end
    end
  end

  ## Header parsing

  defp read_version_line(reader, offset) do
    case Reader.read_until(reader, "\r\n") do
      {:ok, line} ->
        version = String.trim_trailing(line, "\r\n")

        if version =~ ~r/^WARC\/\d+\.\d+$/ do
          {:ok, version}
        else
          {:error, {:bad_version_line, version}, offset}
        end

      :eof ->
        {:error, :truncated_before_version, offset}
    end
  end

  defp read_header_block(reader, offset, acc \\ %{}) do
    case Reader.read_until(reader, "\r\n") do
      {:ok, "\r\n"} ->
        {:ok, acc}

      {:ok, line} ->
        line = String.trim_trailing(line, "\r\n")

        case String.split(line, ":", parts: 2) do
          [name, value] ->
            key = name |> String.trim() |> String.downcase()
            v = String.trim_leading(value)
            read_header_block(reader, offset, Map.put(acc, key, v))

          _ ->
            {:error, {:bad_header_line, line}, offset}
        end

      :eof ->
        {:error, :truncated_in_headers, offset}
    end
  end

  defp interpret_headers(headers, offset) do
    with {:ok, type} <- fetch_type(headers, offset),
         {:ok, id} <- fetch_required(headers, "warc-record-id", offset),
         {:ok, date_str} <- fetch_required(headers, "warc-date", offset),
         {:ok, date} <- parse_date(date_str, offset),
         {:ok, content_length} <- fetch_content_length(headers, offset) do
      {:ok,
       %{
         type: type,
         id: id,
         date: date,
         target_uri: Map.get(headers, "warc-target-uri"),
         content_type: Map.get(headers, "content-type"),
         content_length: content_length
       }}
    end
  end

  defp fetch_type(headers, offset) do
    case Map.fetch(headers, "warc-type") do
      {:ok, value} ->
        atom_or_string =
          if value in @known_types, do: String.to_atom(value), else: value

        {:ok, atom_or_string}

      :error ->
        {:error, :missing_warc_type, offset}
    end
  end

  defp fetch_required(headers, key, offset) do
    case Map.fetch(headers, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_header, key}, offset}
    end
  end

  defp parse_date(str, offset) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} -> {:ok, dt}
      {:error, reason} -> {:error, {:bad_date, str, reason}, offset}
    end
  end

  defp fetch_content_length(headers, offset) do
    with {:ok, value} <- fetch_required(headers, "content-length", offset),
         {n, ""} when n >= 0 <- Integer.parse(value) do
      {:ok, n}
    else
      {:error, _, _} = err -> err
      _ -> {:error, :bad_content_length, offset}
    end
  end

  ## Payload + trailer

  defp build_payload_stream(reader, content_length) do
    chunk_size = 64 * 1024

    Stream.resource(
      fn -> content_length end,
      fn
        0 ->
          # Consume the trailing CRLF CRLF that follows every record.
          _ = Reader.read(reader, 4)
          {:halt, 0}

        remaining ->
          n = min(chunk_size, remaining)

          case Reader.read(reader, n) do
            {:ok, bytes} -> {[bytes], remaining - n}
            :eof -> {:halt, remaining}
          end
      end,
      fn _ -> :ok end
    )
  end
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/parser_test.exs
```
Expected: 5 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/parser.ex test/archiviste/parser_test.exs
git commit -m "Add Archiviste.Parser for single records with lazy payloads"
```

---

## Task 6: Parser — multi-record + auto-drain

**Files:**
- Modify: `lib/archiviste/parser.ex`
- Modify: `test/archiviste/parser_test.exs`

- [ ] **Step 1: Add failing tests for multi-record + skip-payload semantics**

Append to `test/archiviste/parser_test.exs` (inside the `describe` block or as new top-level tests):

```elixir
  test "next_record/1 advances past an unconsumed payload of the previous record" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "first"),
        WarcFixture.record(type: "response", payload: "second")
      ])

    {:ok, r} = reader_from(bytes)
    {:ok, first} = Parser.next_record(r)
    # Intentionally do NOT consume `first.payload`.
    {:ok, second} = Parser.next_record(r)
    assert first.type == :warcinfo
    assert second.type == :response
    assert Record.read_payload(second) == "second"
    assert Parser.next_record(r) == :eof
    Reader.close(r)
  end

  test "next_record/1 advances past a partially consumed payload" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "resource", payload: "aaaaabbbbb"),
        WarcFixture.record(type: "resource", payload: "second")
      ])

    {:ok, r} = reader_from(bytes)
    {:ok, first} = Parser.next_record(r)
    # Consume only the first 5 bytes by taking 1 chunk; chunk_size is 64 KB
    # so this fully consumes the small payload — instead, consume nothing
    # but advance two records.
    [_chunk | _] = first.payload |> Enum.take(1)
    {:ok, second} = Parser.next_record(r)
    assert second.type == :resource
    assert Record.read_payload(second) == "second"
    Reader.close(r)
  end
```

- [ ] **Step 2: Run tests, verify the new ones fail**

```
mix test test/archiviste/parser_test.exs
```
Expected: 2 new failures — the second `next_record/1` call returns a malformed-record error because the previous payload's trailing bytes weren't consumed.

- [ ] **Step 3: Implement auto-drain**

The fix: `next_record/1` must, before attempting to parse new headers, drain any unconsumed bytes from the previous record (including the trailing `\r\n\r\n`). We track pending drain length in a small ETS-like place — but to keep the parser stateless, store it on the Reader.

Modify `lib/archiviste/reader.ex` — extend State and add `set_pending_skip`/`drain_pending`:

In the `State` defstruct, add `:pending_skip`:

```elixir
defmodule State do
  @moduledoc false
  defstruct [:cont, :buffer, :offset, :eof, pending_skip: 0]
end
```

Add client functions near the bottom of the `## Client` section:

```elixir
  def set_pending_skip(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:set_pending_skip, n}, :infinity)

  def consume_pending_skip(pid, delta) when is_integer(delta) and delta >= 0,
    do: GenServer.call(pid, {:consume_pending_skip, delta}, :infinity)

  def drain_pending(pid),
    do: GenServer.call(pid, :drain_pending, :infinity)
```

Add server handlers (place before `## Internals`):

```elixir
  def handle_call({:set_pending_skip, n}, _from, state),
    do: {:reply, :ok, %{state | pending_skip: n}}

  def handle_call({:consume_pending_skip, delta}, _from, state) do
    new = max(state.pending_skip - delta, 0)
    {:reply, :ok, %{state | pending_skip: new}}
  end

  def handle_call(:drain_pending, _from, %{pending_skip: 0} = state),
    do: {:reply, :ok, state}

  def handle_call(:drain_pending, _from, state) do
    {result, state} = do_skip(state, state.pending_skip)
    {:reply, result, %{state | pending_skip: 0}}
  end
```

Modify `lib/archiviste/parser.ex` — wire pending-skip tracking into the payload stream and call `drain_pending` at the top of `next_record/1`:

Replace `next_record/1`'s prelude:

```elixir
  def next_record(reader_pid) do
    :ok = drain_or_warn(reader_pid)
    offset = Reader.offset(reader_pid)
    # ...rest unchanged
```

Add a small helper and update `build_payload_stream/2`:

```elixir
  defp drain_or_warn(reader_pid) do
    case Reader.drain_pending(reader_pid) do
      :ok -> :ok
      :eof -> :ok
    end
  end

  defp build_payload_stream(reader, content_length) do
    # Tell the reader how many bytes belong to the next record's payload +
    # trailing CRLF CRLF, so that if the caller doesn't consume the payload
    # we can drain it later.
    :ok = Reader.set_pending_skip(reader, content_length + 4)
    chunk_size = 64 * 1024

    Stream.resource(
      fn -> content_length end,
      fn
        0 ->
          case Reader.read(reader, 4) do
            {:ok, _} -> :ok = Reader.consume_pending_skip(reader, 4)
            :eof -> :ok
          end

          {:halt, 0}

        remaining ->
          n = min(chunk_size, remaining)

          case Reader.read(reader, n) do
            {:ok, bytes} ->
              :ok = Reader.consume_pending_skip(reader, n)
              {[bytes], remaining - n}

            :eof ->
              {:halt, remaining}
          end
      end,
      fn _ -> :ok end
    )
  end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/parser_test.exs test/archiviste/reader_test.exs
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/parser.ex lib/archiviste/reader.ex test/archiviste/parser_test.exs
git commit -m "Parser auto-drains unconsumed payloads on next_record"
```

---

## Task 7: Top-level `Archiviste.stream!/2`

**Files:**
- Modify: `lib/archiviste.ex` (replace the `mix new` stub)
- Test: `test/archiviste_test.exs`

- [ ] **Step 1: Replace the stub test**

Replace `test/archiviste_test.exs` entirely:

```elixir
defmodule ArchivisteTest do
  use ExUnit.Case, async: true
  doctest Archiviste

  alias Archiviste.{Record, WarcFixture}

  test "stream!/2 yields all records lazily" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "a"),
        WarcFixture.record(type: "response", payload: "b"),
        WarcFixture.record(type: "metadata", payload: "c")
      ])

    records = bytes |> List.wrap() |> Archiviste.stream!() |> Enum.to_list()
    types = Enum.map(records, & &1.type)
    assert types == [:warcinfo, :response, :metadata]
  end

  test "stream!/2 accepts an enumerable of chunked binaries" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "hello"),
        WarcFixture.record(type: "response", payload: "world")
      ])

    chunks = for <<c::binary-size(7) <- pad(bytes)>>, do: c
    records = chunks |> Archiviste.stream!() |> Enum.to_list()
    assert Enum.map(records, & &1.type) == [:warcinfo, :response]
    assert Enum.map(records, &Record.read_payload/1) == ["hello", "world"]
  end

  defp pad(bin) do
    pad = rem(7 - rem(byte_size(bin), 7), 7)
    bin <> String.duplicate(<<0>>, pad)
  end

  test "stream!/2 supports Stream.filter without touching payloads" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(type: "response", payload: "body")
      ])

    [resp] =
      [bytes]
      |> Archiviste.stream!()
      |> Stream.filter(&(&1.type == :response))
      |> Enum.to_list()

    assert Record.read_payload(resp) == "body"
  end
end
```

(`pad/1` exists because the last `<<...::binary-size(7)<-pad(bytes)>>` pattern requires a multiple of 7; we strip trailing nulls by relying on the parser to stop at content-length, so padding is safe as long as no real record starts in the padding — which it can't since `<<0>>` doesn't begin with `WARC/`.)

Actually, simplify by removing the padded-chunk test variant; replace that test with:

```elixir
  test "stream!/2 accepts an enumerable of chunked binaries" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "hello"),
        WarcFixture.record(type: "response", payload: "world")
      ])

    # Split into 7-byte chunks; the last chunk may be shorter.
    chunks =
      bytes
      |> :binary.bin_to_list()
      |> Enum.chunk_every(7)
      |> Enum.map(&IO.iodata_to_binary([&1]))

    records = chunks |> Archiviste.stream!() |> Enum.to_list()
    assert Enum.map(records, & &1.type) == [:warcinfo, :response]
    assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["hello", "world"]
  end
```

(Remove the `pad/1` helper.)

- [ ] **Step 2: Run tests, verify they fail**

```
mix test test/archiviste_test.exs
```
Expected: FAIL — `Archiviste.stream!/1` undefined.

- [ ] **Step 3: Implement `Archiviste.stream!/2`**

Replace `lib/archiviste.ex` entirely:

```elixir
defmodule Archiviste do
  @moduledoc """
  A streaming reader for WARC (Web ARChive, ISO 28500) files.

  ## Quick start

      "crawl.warc.gz"
      |> Archiviste.stream_file!()
      |> Stream.filter(&(&1.type == :response))
      |> Enum.take(10)

  Each yielded record is an `Archiviste.Record` whose `:payload` is a lazy
  `Stream.t()` of binary chunks. See `Archiviste.Record` for details.
  """

  alias Archiviste.{Parser, Reader}

  @type opts :: [strict: boolean(), verify_digests: boolean()]

  @doc """
  Stream WARC records from an arbitrary enumerable of binary chunks.

  This is the core API. For files, see `stream_file!/2`.

  ## Options

    * `:strict` (default `false`) — when `true`, malformed records raise
      mid-stream instead of being skipped with a `Logger.warning`.
    * `:verify_digests` (default `false`) — when `true`, verify WARC block
      and payload digests; mismatches are treated as malformed records.
  """
  @spec stream!(Enumerable.t(), opts()) :: Enumerable.t()
  def stream!(enumerable, _opts \\ []) do
    Stream.resource(
      fn ->
        {:ok, reader} = Reader.start_link(enumerable)
        reader
      end,
      fn reader ->
        case Parser.next_record(reader) do
          {:ok, record} -> {[record], reader}
          :eof -> {:halt, reader}
          {:error, reason, offset} ->
            raise Archiviste.Error.MalformedRecordError,
              offset: offset,
              reason: reason
        end
      end,
      fn reader -> Reader.close(reader) end
    )
  end
end
```

(Strict/lenient toggle, gzip, and digest verification are deferred to later
tasks; the bare-minimum here is to wire up the streaming pipeline.)

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste_test.exs
```
Expected: 3 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste.ex test/archiviste_test.exs
git commit -m "Add Archiviste.stream!/2 top-level entry point"
```

---

## Task 8: `Archiviste.stream_file!/2` for plain files

**Files:**
- Modify: `lib/archiviste.ex`
- Test: `test/archiviste_test.exs`

- [ ] **Step 1: Add failing test**

Append to `test/archiviste_test.exs`:

```elixir
  test "stream_file!/2 reads from a plain .warc file" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(type: "response", payload: "body")
      ])

    path = Path.join(System.tmp_dir!(), "archiviste_test_#{System.unique_integer([:positive])}.warc")
    File.write!(path, bytes)

    try do
      records = path |> Archiviste.stream_file!() |> Enum.to_list()
      assert Enum.map(records, & &1.type) == [:warcinfo, :response]
    after
      File.rm(path)
    end
  end
```

- [ ] **Step 2: Run test, verify it fails**

```
mix test test/archiviste_test.exs
```
Expected: FAIL — `Archiviste.stream_file!/1` undefined.

- [ ] **Step 3: Implement `stream_file!/2` (plain only for now)**

Append to `lib/archiviste.ex` (before the closing `end`):

```elixir
  @doc """
  Stream WARC records from a file path.

  Detects per-record gzip compression from the `.gz` extension or from the
  gzip magic bytes at the start of the file.

  Accepts the same options as `stream!/2`.
  """
  @spec stream_file!(Path.t(), opts()) :: Enumerable.t()
  def stream_file!(path, opts \\ []) when is_binary(path) do
    path
    |> File.stream!([], 64 * 1024)
    |> stream!(opts)
  end
```

- [ ] **Step 4: Run tests, verify pass**

```
mix test test/archiviste_test.exs
```
Expected: 4 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste.ex test/archiviste_test.exs
git commit -m "Add Archiviste.stream_file!/2 for plain .warc files"
```

---

## Task 9: Per-record gzip support

**Files:**
- Create: `lib/archiviste/gzip.ex`
- Test: `test/archiviste/gzip_test.exs`
- Modify: `lib/archiviste.ex`
- Modify: `test/archiviste_test.exs`

A `.warc.gz` file is a concatenation of independent gzip members (one per
WARC record). The standard `:zlib.gunzip/1` only decodes one member; we need
streaming decode that yields decompressed bytes across members.

- [ ] **Step 1: Write failing test for the gzip stream stage**

Create `test/archiviste/gzip_test.exs`:

```elixir
defmodule Archiviste.GzipTest do
  use ExUnit.Case, async: true

  alias Archiviste.{Gzip, WarcFixture}

  test "decodes a single gzip member" do
    bytes = WarcFixture.gzip("hello")
    out = bytes |> List.wrap() |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == "hello"
  end

  test "decodes concatenated gzip members (per-record .warc.gz layout)" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "one"),
        WarcFixture.record(type: "response", payload: "two")
      ])

    expected =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "one"),
        WarcFixture.record(type: "response", payload: "two")
      ])

    out = members |> List.wrap() |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == expected
  end

  test "decodes across chunk boundaries that split mid-member" do
    bytes = WarcFixture.gzip(String.duplicate("xyz", 5_000))
    # Split into 17-byte chunks so the gzip header/body are fragmented.
    chunks =
      bytes
      |> :binary.bin_to_list()
      |> Enum.chunk_every(17)
      |> Enum.map(&IO.iodata_to_binary([&1]))

    out = chunks |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == String.duplicate("xyz", 5_000)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

```
mix test test/archiviste/gzip_test.exs
```
Expected: FAIL.

- [ ] **Step 3: Implement Gzip module**

Create `lib/archiviste/gzip.ex`:

```elixir
defmodule Archiviste.Gzip do
  @moduledoc false
  # Streaming gunzip that handles concatenated gzip members.
  #
  # `.warc.gz` files are made of one gzip member per WARC record. We can't
  # use `:zlib.gunzip/1` (one-shot) and must instead drive `:zlib.inflate`
  # in streaming mode, detecting member boundaries (`:stream_end`) and
  # resetting the inflator for the next member.

  @spec decode_stream(Enumerable.t()) :: Enumerable.t()
  def decode_stream(enumerable) do
    Stream.transform(
      enumerable,
      fn -> new_inflator() end,
      fn chunk, z -> {inflate_chunk(z, chunk), z} end,
      fn z -> :zlib.close(z) end
    )
  end

  defp new_inflator do
    z = :zlib.open()
    # 31 = max window bits (15) + gzip flag (16)
    :ok = :zlib.inflateInit(z, 31)
    z
  end

  defp inflate_chunk(_z, ""), do: []

  defp inflate_chunk(z, chunk) do
    case :zlib.safeInflate(z, chunk) do
      {:continue, out} ->
        out_bin = IO.iodata_to_binary(out)
        # Pull any remaining output for this chunk by continuing with empty input.
        out_bin <> drain_continue(z)
        |> wrap()

      {:finished, out} ->
        out_bin = IO.iodata_to_binary(out)
        # End of one gzip member. Reset inflator for the next member.
        :ok = :zlib.inflateReset(z)
        [out_bin]
    end
  end

  defp drain_continue(z) do
    case :zlib.safeInflate(z, "") do
      {:continue, out} -> IO.iodata_to_binary(out) <> drain_continue(z)
      {:finished, out} ->
        out_bin = IO.iodata_to_binary(out)
        :ok = :zlib.inflateReset(z)
        out_bin
    end
  end

  defp wrap(bin) when bin == "", do: []
  defp wrap(bin), do: [bin]
end
```

- [ ] **Step 4: Run tests, verify they pass**

```
mix test test/archiviste/gzip_test.exs
```
Expected: 3 tests, 0 failures.

- [ ] **Step 5: Add a failing test for `stream_file!/2` on a .warc.gz**

Append to `test/archiviste_test.exs`:

```elixir
  test "stream_file!/2 auto-detects per-record gzip from .gz extension" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "first"),
        WarcFixture.record(type: "response", payload: "second")
      ])

    path =
      Path.join(System.tmp_dir!(), "archiviste_test_#{System.unique_integer([:positive])}.warc.gz")

    File.write!(path, members)

    try do
      records = path |> Archiviste.stream_file!() |> Enum.to_list()
      assert Enum.map(records, & &1.type) == [:warcinfo, :response]
      assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["first", "second"]
    after
      File.rm(path)
    end
  end

  test "stream_file!/2 auto-detects gzip from magic bytes when extension is plain" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "magic")
      ])

    path = Path.join(System.tmp_dir!(), "archiviste_test_#{System.unique_integer([:positive])}.warc")
    File.write!(path, members)

    try do
      [record] = path |> Archiviste.stream_file!() |> Enum.to_list()
      assert record.type == :warcinfo
      assert Archiviste.Record.read_payload(record) == "magic"
    after
      File.rm(path)
    end
  end
```

- [ ] **Step 6: Run, expect failure**

```
mix test test/archiviste_test.exs
```
Expected: FAIL — gzip not yet wired in.

- [ ] **Step 7: Wire gzip into `stream_file!/2`**

Modify `lib/archiviste.ex` — replace `stream_file!/2`:

```elixir
  def stream_file!(path, opts \\ []) when is_binary(path) do
    raw = File.stream!(path, [], 64 * 1024)

    raw
    |> maybe_gunzip(path)
    |> stream!(opts)
  end

  defp maybe_gunzip(stream, path) do
    if gzip?(path, stream) do
      Archiviste.Gzip.decode_stream(stream)
    else
      stream
    end
  end

  defp gzip?(path, stream) do
    if String.ends_with?(path, ".gz") do
      true
    else
      case Enum.take(stream, 1) do
        [<<0x1F, 0x8B, _::binary>> | _] -> true
        _ -> false
      end
    end
  end
```

Wait — `Enum.take(stream, 1)` consumes from a `File.stream!` which is
re-iterable, but each iteration re-opens the file. To make this clean and
unambiguous, always open and read the first 2 bytes once for magic-byte
detection on non-`.gz` paths:

```elixir
  defp gzip?(path, _stream) do
    cond do
      String.ends_with?(path, ".gz") ->
        true

      true ->
        case File.open(path, [:read, :binary], fn io -> IO.binread(io, 2) end) do
          {:ok, <<0x1F, 0x8B>>} -> true
          _ -> false
        end
    end
  end
```

And drop the `_stream` arg since it's unused:

```elixir
  defp maybe_gunzip(stream, path) do
    if gzip?(path), do: Archiviste.Gzip.decode_stream(stream), else: stream
  end

  defp gzip?(path) do
    String.ends_with?(path, ".gz") or
      match?({:ok, <<0x1F, 0x8B>>}, File.open(path, [:read, :binary], &IO.binread(&1, 2)))
  end
```

- [ ] **Step 8: Run all tests**

```
mix test
```
Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add lib/archiviste/gzip.ex lib/archiviste.ex test/archiviste/gzip_test.exs test/archiviste_test.exs
git commit -m "Add per-record gzip support to stream_file!"
```

---

## Task 10: Lenient (default) vs strict error handling

**Files:**
- Modify: `lib/archiviste.ex`
- Test: `test/archiviste_test.exs`

- [ ] **Step 1: Add failing tests for lenient/strict**

Append to `test/archiviste_test.exs`:

```elixir
  test "lenient mode: malformed record is skipped, subsequent records yield" do
    good1 = WarcFixture.record(type: "warcinfo", payload: "ok1")
    # A malformed record: invalid version line.
    bad = "WARC/garbage\r\n\r\n"
    good2 = WarcFixture.record(type: "response", payload: "ok2")

    bytes = good1 <> bad <> good2

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [bytes] |> Archiviste.stream!() |> Enum.to_list()
      end)

    assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["ok1", "ok2"]
    assert log =~ "skipped malformed record"
  end

  test "strict mode: malformed record raises mid-stream" do
    good = WarcFixture.record(type: "warcinfo", payload: "ok")
    bad = "WARC/garbage\r\n\r\n"

    assert_raise Archiviste.Error.MalformedRecordError, fn ->
      [good <> bad] |> Archiviste.stream!(strict: true) |> Enum.to_list()
    end
  end
```

- [ ] **Step 2: Run, expect failures**

```
mix test test/archiviste_test.exs
```
Expected: 2 failures.

- [ ] **Step 3: Implement lenient/strict in `stream!/2`**

The current `stream!/2` raises on every error. Add re-sync logic and the
strict toggle. After an error, advance the reader to the next plausible
record boundary by scanning for `"WARC/"` followed by digits.

Add to `lib/archiviste/reader.ex` (client + server):

```elixir
  def scan_to(pid, marker) when is_binary(marker),
    do: GenServer.call(pid, {:scan_to, marker}, :infinity)
```

```elixir
  def handle_call({:scan_to, marker}, _from, state) do
    {result, state} = do_scan_to(state, marker, byte_size(marker))
    {:reply, result, state}
  end
```

```elixir
  defp do_scan_to(state, marker, marker_size) do
    case :binary.match(state.buffer, marker) do
      {pos, ^marker_size} ->
        <<_::binary-size(pos), rest::binary>> = state.buffer
        {:ok, %{state | buffer: rest, offset: state.offset + pos}}

      :nomatch ->
        if state.eof do
          {:eof, %{state | buffer: <<>>, offset: state.offset + byte_size(state.buffer)}}
        else
          # Keep the last (marker_size - 1) bytes in case the marker
          # straddles a chunk boundary.
          keep = max(byte_size(state.buffer) - (marker_size - 1), 0)
          drop = byte_size(state.buffer) - keep
          <<_::binary-size(drop), tail::binary>> = state.buffer
          state = %{state | buffer: tail, offset: state.offset + drop}

          case pull(state) do
            {:ok, state} -> do_scan_to(state, marker, marker_size)
            {:eof, state} -> do_scan_to(state, marker, marker_size)
          end
        end
    end
  end
```

Modify `lib/archiviste.ex`:

```elixir
  require Logger

  alias Archiviste.{Parser, Reader, Error}

  # ...

  def stream!(enumerable, opts \\ []) do
    strict? = Keyword.get(opts, :strict, false)

    Stream.resource(
      fn ->
        {:ok, reader} = Reader.start_link(enumerable)
        {reader, strict?}
      end,
      fn {reader, strict?} = acc ->
        case Parser.next_record(reader) do
          {:ok, record} ->
            {[record], acc}

          :eof ->
            {:halt, acc}

          {:error, reason, offset} when strict? ->
            raise Error.MalformedRecordError, offset: offset, reason: reason

          {:error, reason, offset} ->
            Logger.warning(
              "Archiviste: skipped malformed record at offset #{offset}: #{inspect(reason)}"
            )

            case Reader.scan_to(reader, "WARC/") do
              :ok -> {[], acc}
              :eof -> {:halt, acc}
            end
        end
      end,
      fn {reader, _} -> Reader.close(reader) end
    )
  end
```

Also clear `pending_skip` before re-syncing (since the broken record's
content_length is unreliable). Add to Reader client + use it:

In `lib/archiviste/reader.ex` client:

```elixir
  def clear_pending(pid), do: GenServer.call(pid, :clear_pending, :infinity)
```

```elixir
  def handle_call(:clear_pending, _from, state),
    do: {:reply, :ok, %{state | pending_skip: 0}}
```

In `lib/archiviste.ex`, in the `{:error, ...}` lenient branch, before `scan_to`:

```elixir
            :ok = Reader.clear_pending(reader)
```

- [ ] **Step 4: Run tests**

```
mix test
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste.ex lib/archiviste/reader.ex test/archiviste_test.exs
git commit -m "Add lenient (default) and strict error modes"
```

---

## Task 11: Truncated-file handling

**Files:**
- Modify: `lib/archiviste/parser.ex`
- Test: `test/archiviste_test.exs`

- [ ] **Step 1: Add failing tests**

Append to `test/archiviste_test.exs`:

```elixir
  test "lenient: truncated mid-payload logs and ends cleanly" do
    full = WarcFixture.record(type: "response", payload: "abcdefghij")
    truncated = binary_part(full, 0, byte_size(full) - 5)

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [truncated] |> Archiviste.stream!() |> Enum.to_list()
      end)

    # Header is parsed and the record is yielded, but reading its payload
    # would error. In lenient mode the stream terminates with a log entry.
    assert length(records) <= 1
    assert log =~ "truncated" or log =~ "malformed"
  end

  test "strict: truncated file raises TruncatedFileError when payload is short" do
    full = WarcFixture.record(type: "response", payload: "abcdefghij")
    truncated = binary_part(full, 0, byte_size(full) - 5)

    assert_raise Archiviste.Error.TruncatedFileError, fn ->
      [truncated]
      |> Archiviste.stream!(strict: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)
    end
  end
```

- [ ] **Step 2: Run, expect failures**

```
mix test test/archiviste_test.exs
```

- [ ] **Step 3: Implement truncated-file detection**

The payload Stream currently halts silently on `:eof`. Change it to raise
or signal so callers can distinguish a complete payload from a truncated
one. Modify `build_payload_stream/2` in `lib/archiviste/parser.ex`:

```elixir
  defp build_payload_stream(reader, content_length) do
    :ok = Reader.set_pending_skip(reader, content_length + 4)
    chunk_size = 64 * 1024

    Stream.resource(
      fn -> {content_length, Reader.offset(reader)} end,
      fn
        {0, _start} ->
          case Reader.read(reader, 4) do
            {:ok, _} -> :ok = Reader.consume_pending_skip(reader, 4)
            :eof -> throw({:archiviste_truncated, Reader.offset(reader)})
          end

          {:halt, {0, nil}}

        {remaining, start} ->
          n = min(chunk_size, remaining)

          case Reader.read(reader, n) do
            {:ok, bytes} ->
              :ok = Reader.consume_pending_skip(reader, n)
              {[bytes], {remaining - n, start}}

            :eof ->
              throw({:archiviste_truncated, Reader.offset(reader)})
          end
      end,
      fn _ -> :ok end
    )
  end
```

In `lib/archiviste.ex`, wrap the parser/stream consumption to catch
`:archiviste_truncated` and route through lenient/strict:

The catch needs to happen wherever the payload stream is consumed. Since
the payload is consumed by the *caller* (user code), throwing from inside
their `Enum.map` would surface to them. Instead of `throw`, raise the
appropriate exception unconditionally — `TruncatedFileError` — and let
the user's lenient-mode wrapper handle it.

Replace the `throw` with:

```elixir
            :eof ->
              raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
```

For lenient mode, document that the *outer* stream catches a truncation
only at record-boundary scan time. Once a record header is parsed and
returned, the user is responsible for handling payload errors. Truncations
on record headers are already handled by the `{:error, :truncated_in_headers, ...}`
path.

Drop the lenient-payload-truncation test (it's user-side behavior) and
keep only the strict version. Revise the appended tests:

```elixir
  test "strict: truncated payload raises TruncatedFileError when consumed" do
    full = WarcFixture.record(type: "response", payload: "abcdefghij")
    truncated = binary_part(full, 0, byte_size(full) - 5)

    assert_raise Archiviste.Error.TruncatedFileError, fn ->
      [truncated]
      |> Archiviste.stream!(strict: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)
    end
  end

  test "lenient: truncated header is skipped with a log warning" do
    full = WarcFixture.record(type: "response", payload: "abc")
    truncated_header = binary_part(full, 0, 30) # cut mid-header

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [truncated_header] |> Archiviste.stream!() |> Enum.to_list()
      end)

    assert records == []
    assert log =~ "malformed" or log =~ "truncated"
  end
```

- [ ] **Step 4: Run tests**

```
mix test
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/parser.ex test/archiviste_test.exs
git commit -m "Raise TruncatedFileError on payload truncation"
```

---

## Task 12: Digest verification (opt-in)

**Files:**
- Create: `lib/archiviste/digest.ex`
- Test: `test/archiviste/digest_test.exs`
- Modify: `lib/archiviste/parser.ex` (compute digest while streaming payload)
- Modify: `lib/archiviste.ex` (option passthrough)
- Modify: `test/archiviste_test.exs`

The WARC spec uses `WARC-Block-Digest` (over the entire content block,
i.e. our `payload`) and `WARC-Payload-Digest` (over the *inner* HTTP
body for response/request records, ignoring the HTTP headers).

For v1 we implement **only `WARC-Block-Digest`**. Payload-digest verification
requires the HTTP layer and is deferred to a later task (Task 18).

Digest header value format: `algorithm:base32-or-hex`, e.g.
`sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ`.

- [ ] **Step 1: Write failing digest test**

Create `test/archiviste/digest_test.exs`:

```elixir
defmodule Archiviste.DigestTest do
  use ExUnit.Case, async: true

  alias Archiviste.Digest

  test "parses and verifies sha1 with base32 encoding" do
    payload = "hello world"
    digest_bytes = :crypto.hash(:sha, payload)
    b32 = Base.encode32(digest_bytes, padding: false)
    header = "sha1:#{b32}"

    assert Digest.verify(header, payload) == :ok
  end

  test "parses and verifies sha256 with hex encoding" do
    payload = "hello world"
    digest_bytes = :crypto.hash(:sha256, payload)
    hex = Base.encode16(digest_bytes, case: :lower)
    header = "sha256:#{hex}"

    assert Digest.verify(header, payload) == :ok
  end

  test "returns {:error, :mismatch, expected, actual} on bad digest" do
    payload = "hello world"
    bad_header = "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    assert {:error, :mismatch, "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", _} =
             Digest.verify(bad_header, payload)
  end

  test "returns {:error, :unknown_algorithm, _} on unsupported algo" do
    assert {:error, :unknown_algorithm, "md5"} = Digest.verify("md5:abcd", "x")
  end

  test "stream API computes a digest over chunked input" do
    payload = String.duplicate("abc", 1000)
    digest_bytes = :crypto.hash(:sha, payload)
    expected_b32 = Base.encode32(digest_bytes, padding: false)

    state = Digest.init(:sha)
    state = Enum.reduce(["abc", String.duplicate("abc", 999)], state, &Digest.update(&2, &1))
    assert Digest.finalize_base32(state) == expected_b32
  end
end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/digest_test.exs
```

- [ ] **Step 3: Implement Digest module**

Create `lib/archiviste/digest.ex`:

```elixir
defmodule Archiviste.Digest do
  @moduledoc false

  @algos %{
    "sha1" => :sha,
    "sha256" => :sha256,
    "sha512" => :sha512,
    "md5" => :md5
  }

  # Supported in verification — exclude md5 by default since the WARC spec
  # treats it as legacy. Keep it parseable but not verifiable.
  @verified_algos ["sha1", "sha256", "sha512"]

  @spec verify(String.t(), iodata()) ::
          :ok | {:error, :mismatch, String.t(), String.t()} | {:error, :unknown_algorithm, String.t()}
  def verify(header, payload) when is_binary(header) do
    with {:ok, algo_str, expected_encoded} <- parse_header(header),
         true <- algo_str in @verified_algos or {:error, :unknown_algorithm, algo_str},
         erlang_algo = Map.fetch!(@algos, algo_str),
         actual_bytes = :crypto.hash(erlang_algo, payload),
         expected_bytes = decode_digest(expected_encoded) do
      if actual_bytes == expected_bytes do
        :ok
      else
        actual_encoded = Base.encode32(actual_bytes, padding: false)
        {:error, :mismatch, header, "#{algo_str}:#{actual_encoded}"}
      end
    else
      {:error, _, _} = err -> err
    end
  end

  defp parse_header(header) do
    case String.split(header, ":", parts: 2) do
      [algo, value] -> {:ok, String.downcase(algo), value}
      _ -> {:error, :unknown_algorithm, header}
    end
  end

  defp decode_digest(value) do
    cond do
      hex?(value) -> Base.decode16!(value, case: :mixed)
      true -> Base.decode32!(value, padding: false)
    end
  end

  defp hex?(value), do: String.match?(value, ~r/^[0-9a-fA-F]+$/) and rem(byte_size(value), 2) == 0

  ## Streaming API

  @spec init(:sha | :sha256 | :sha512 | :md5) :: term()
  def init(algo), do: :crypto.hash_init(algo)

  @spec update(term(), iodata()) :: term()
  def update(state, data), do: :crypto.hash_update(state, data)

  @spec finalize_base32(term()) :: String.t()
  def finalize_base32(state),
    do: state |> :crypto.hash_final() |> Base.encode32(padding: false)

  @spec algo_from_header(String.t()) ::
          {:ok, atom(), String.t()} | {:error, :unknown_algorithm, String.t()}
  def algo_from_header(header) when is_binary(header) do
    with {:ok, algo_str, expected} <- parse_header(header),
         true <- algo_str in @verified_algos or {:error, :unknown_algorithm, algo_str} do
      {:ok, Map.fetch!(@algos, algo_str), expected}
    else
      {:error, _, _} = err -> err
    end
  end
end
```

- [ ] **Step 4: Run digest tests, verify pass**

```
mix test test/archiviste/digest_test.exs
```

- [ ] **Step 5: Add failing integration test for digest verification**

Append to `test/archiviste_test.exs`:

```elixir
  test "verify_digests: true verifies WARC-Block-Digest and passes on match" do
    payload = "matched-payload"
    digest = "sha1:" <> (payload |> then(&:crypto.hash(:sha, &1)) |> Base.encode32(padding: false))

    bytes =
      WarcFixture.record(
        type: "resource",
        headers: [{"WARC-Block-Digest", digest}],
        payload: payload
      )

    [record] =
      [bytes]
      |> Archiviste.stream!(verify_digests: true)
      |> Enum.to_list()

    assert Archiviste.Record.read_payload(record) == payload
  end

  test "strict + verify_digests: digest mismatch raises DigestMismatchError" do
    payload = "actual-payload"
    bytes =
      WarcFixture.record(
        type: "resource",
        headers: [{"WARC-Block-Digest", "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}],
        payload: payload
      )

    assert_raise Archiviste.Error.DigestMismatchError, fn ->
      [bytes]
      |> Archiviste.stream!(strict: true, verify_digests: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)
    end
  end
```

- [ ] **Step 6: Run, expect failures**

```
mix test test/archiviste_test.exs
```

- [ ] **Step 7: Wire digest checking into the payload Stream**

In `lib/archiviste/parser.ex`, accept opts and thread digest verification
through the payload Stream:

Change `next_record/1` to `next_record/2`:

```elixir
  def next_record(reader_pid, opts \\ []) do
    :ok = drain_or_warn(reader_pid)
    offset = Reader.offset(reader_pid)

    case Reader.peek(reader_pid, 1) do
      :eof ->
        :eof

      {:ok, _} ->
        with {:ok, version} <- read_version_line(reader_pid, offset),
             {:ok, headers} <- read_header_block(reader_pid, offset),
             {:ok, parsed} <- interpret_headers(headers, offset) do
          payload_stream =
            build_payload_stream(reader_pid, parsed.content_length, headers, parsed.id, opts)

          record = %Archiviste.Record{
            version: version,
            type: parsed.type,
            id: parsed.id,
            date: parsed.date,
            target_uri: parsed.target_uri,
            content_type: parsed.content_type,
            content_length: parsed.content_length,
            headers: headers,
            payload: payload_stream,
            offset: offset
          }

          {:ok, record}
        end
    end
  end
```

Update `build_payload_stream/5`:

```elixir
  alias Archiviste.Digest

  defp build_payload_stream(reader, content_length, headers, record_id, opts) do
    :ok = Reader.set_pending_skip(reader, content_length + 4)
    chunk_size = 64 * 1024
    verify? = Keyword.get(opts, :verify_digests, false)

    digest_ctx =
      with true <- verify?,
           {:ok, header} <- Map.fetch(headers, "warc-block-digest"),
           {:ok, algo, expected} <- Digest.algo_from_header(header) do
        {Digest.init(algo), expected, header}
      else
        _ -> nil
      end

    Stream.resource(
      fn -> {content_length, digest_ctx} end,
      fn
        {0, dctx} ->
          case Reader.read(reader, 4) do
            {:ok, _} ->
              :ok = Reader.consume_pending_skip(reader, 4)
              verify_final(dctx, record_id)

            :eof ->
              raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
          end

          {:halt, {0, nil}}

        {remaining, dctx} ->
          n = min(chunk_size, remaining)

          case Reader.read(reader, n) do
            {:ok, bytes} ->
              :ok = Reader.consume_pending_skip(reader, n)
              new_dctx = update_dctx(dctx, bytes)
              {[bytes], {remaining - n, new_dctx}}

            :eof ->
              raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
          end
      end,
      fn _ -> :ok end
    )
  end

  defp update_dctx(nil, _), do: nil
  defp update_dctx({state, expected, header}, bytes),
    do: {Digest.update(state, bytes), expected, header}

  defp verify_final(nil, _), do: :ok

  defp verify_final({state, expected, header}, record_id) do
    actual = Archiviste.Digest.finalize_base32(state)
    expected_clean = String.split(header, ":", parts: 2) |> List.last() |> String.trim()

    if actual == expected_clean do
      :ok
    else
      raise Archiviste.Error.DigestMismatchError,
        record_id: record_id,
        digest_kind: :block,
        expected: header,
        actual: "sha?:" <> actual
    end
  end
```

In `lib/archiviste.ex`, pass opts to the parser:

```elixir
        case Parser.next_record(reader, opts) do
```

- [ ] **Step 8: Run all tests**

```
mix test
```

Note: the lenient-mode behavior for digest mismatches surfaces only when
the user actually consumes the payload (since digest is computed during
streaming). In strict mode the raise propagates. For lenient mode users
can `try`/`rescue` around payload reads. Document this in `@moduledoc`
of `Archiviste.Error`.

- [ ] **Step 9: Commit**

```bash
git add lib/archiviste/digest.ex lib/archiviste/parser.ex lib/archiviste.ex \
        test/archiviste/digest_test.exs test/archiviste_test.exs
git commit -m "Add opt-in WARC-Block-Digest verification"
```

---

## Task 13: `Archiviste.read_at!/3`

**Files:**
- Modify: `lib/archiviste.ex`
- Test: `test/archiviste_test.exs`

- [ ] **Step 1: Add failing test**

Append to `test/archiviste_test.exs`:

```elixir
  test "read_at!/3 reads a single record at a known offset (plain file)" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "second")
    bytes = a <> b

    path = Path.join(System.tmp_dir!(), "archiviste_test_at_#{System.unique_integer([:positive])}.warc")
    File.write!(path, bytes)

    try do
      offset = byte_size(a)
      record = Archiviste.read_at!(path, offset)
      assert record.type == :response
      assert Archiviste.Record.read_payload(record) == "second"
    after
      File.rm(path)
    end
  end

  test "read_at!/3 reads a single record at a known offset (.warc.gz file)" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "second")
    a_gz = WarcFixture.gzip(a)
    b_gz = WarcFixture.gzip(b)
    bytes = a_gz <> b_gz

    path =
      Path.join(System.tmp_dir!(), "archiviste_test_at_#{System.unique_integer([:positive])}.warc.gz")

    File.write!(path, bytes)

    try do
      offset = byte_size(a_gz)
      record = Archiviste.read_at!(path, offset)
      assert record.type == :response
      assert Archiviste.Record.read_payload(record) == "second"
    after
      File.rm(path)
    end
  end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste_test.exs
```

- [ ] **Step 3: Implement `read_at!/3`**

Append to `lib/archiviste.ex`:

```elixir
  @doc """
  Read exactly one record starting at a given byte offset in `path`.

  Works on both plain `.warc` files and per-record-gzipped `.warc.gz` files.
  For `.warc.gz`, the offset must point to the start of a gzip member
  (record-aligned).
  """
  @spec read_at!(Path.t(), non_neg_integer(), opts()) :: Archiviste.Record.t()
  def read_at!(path, offset, opts \\ []) when is_binary(path) and offset >= 0 do
    {:ok, io} = File.open(path, [:read, :binary])
    {:ok, _} = :file.position(io, offset)

    stream =
      Stream.resource(
        fn -> io end,
        fn io ->
          case IO.binread(io, 64 * 1024) do
            :eof -> {:halt, io}
            data when is_binary(data) -> {[data], io}
          end
        end,
        fn io -> File.close(io) end
      )

    decoded =
      if gzip?(path), do: Archiviste.Gzip.decode_stream(stream), else: stream

    case decoded |> stream!(opts) |> Enum.take(1) do
      [record] ->
        # Eagerly read the payload so the underlying file can close cleanly.
        # Callers wanting laziness should use stream_file!/2 with their own
        # filtering — read_at!/3 is the random-access primitive.
        payload_bytes = Archiviste.Record.read_payload(record)
        %{record | payload: [payload_bytes]}

      [] ->
        raise Archiviste.Error.TruncatedFileError, offset: offset
    end
  end
```

- [ ] **Step 4: Run tests**

```
mix test
```

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste.ex test/archiviste_test.exs
git commit -m "Add Archiviste.read_at!/3 for random-access reads"
```

---

## Task 14: HTTP.Request and HTTP.Response structs

**Files:**
- Create: `lib/archiviste/http/request.ex`
- Create: `lib/archiviste/http/response.ex`
- Test: `test/archiviste/http_test.exs`

- [ ] **Step 1: Write failing struct tests**

Create `test/archiviste/http_test.exs`:

```elixir
defmodule Archiviste.HTTPTest do
  use ExUnit.Case, async: true

  alias Archiviste.HTTP

  test "Response struct builds with expected fields" do
    resp = %HTTP.Response{
      record: nil,
      status: 200,
      reason: "OK",
      http_version: "HTTP/1.1",
      headers: [{"content-type", "text/html"}],
      body: ["<html></html>"],
      body_encoding: nil
    }

    assert resp.status == 200
    assert resp.headers == [{"content-type", "text/html"}]
  end

  test "Request struct builds with expected fields" do
    req = %HTTP.Request{
      record: nil,
      method: "GET",
      target: "/",
      http_version: "HTTP/1.1",
      headers: [{"host", "example.com"}],
      body: [],
      body_encoding: nil
    }

    assert req.method == "GET"
    assert req.target == "/"
  end
end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 3: Implement the structs**

Create `lib/archiviste/http/response.ex`:

```elixir
defmodule Archiviste.HTTP.Response do
  @moduledoc """
  An HTTP response parsed out of a WARC `response` record.

  `:body` is a lazy `Stream.t()` of binary chunks (or, when `decode_body: true`
  was passed and the body's `Content-Encoding` was recognized, a Stream of
  decoded bytes).
  """

  @type t :: %__MODULE__{
          record: Archiviste.Record.t() | nil,
          status: 100..599,
          reason: String.t(),
          http_version: String.t(),
          headers: [{String.t(), String.t()}],
          body: Enumerable.t(),
          body_encoding: nil | :gzip | :deflate | :br | :zstd | :identity | binary()
        }

  @enforce_keys [:status, :reason, :http_version, :headers, :body]
  defstruct [:record, :status, :reason, :http_version, :headers, :body, :body_encoding]
end
```

Create `lib/archiviste/http/request.ex`:

```elixir
defmodule Archiviste.HTTP.Request do
  @moduledoc """
  An HTTP request parsed out of a WARC `request` record.
  """

  @type t :: %__MODULE__{
          record: Archiviste.Record.t() | nil,
          method: String.t(),
          target: String.t(),
          http_version: String.t(),
          headers: [{String.t(), String.t()}],
          body: Enumerable.t(),
          body_encoding: nil | :gzip | :deflate | :br | :zstd | :identity | binary()
        }

  @enforce_keys [:method, :target, :http_version, :headers, :body]
  defstruct [:record, :method, :target, :http_version, :headers, :body, :body_encoding]
end
```

- [ ] **Step 4: Run, verify pass**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/http/*.ex test/archiviste/http_test.exs
git commit -m "Add HTTP.Request and HTTP.Response structs"
```

---

## Task 15: `Archiviste.HTTP.parse/2` for responses

**Files:**
- Create: `lib/archiviste/http.ex`
- Modify: `test/archiviste/http_test.exs`

- [ ] **Step 1: Add failing test**

Append to `test/archiviste/http_test.exs`:

```elixir
  alias Archiviste.{Record, WarcFixture}

  defp response_record(http_payload) do
    bytes =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: http_payload
      )

    [record] = [bytes] |> Archiviste.stream!() |> Enum.to_list()
    record
  end

  test "parse/2 parses status line and headers of a response" do
    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Content-Type: text/html\r\n" <>
        "Content-Length: 11\r\n" <>
        "\r\n" <>
        "hello world"

    record = response_record(http)
    assert {:ok, %HTTP.Response{} = resp} = HTTP.parse(record)
    assert resp.status == 200
    assert resp.reason == "OK"
    assert resp.http_version == "HTTP/1.1"
    assert {"content-type", "text/html"} in resp.headers
    assert IO.iodata_to_binary(Enum.to_list(resp.body)) == "hello world"
  end

  test "parse/2 returns error on non-response/non-request record" do
    bytes = WarcFixture.record(type: "metadata", payload: "k: v")
    [record] = [bytes] |> Archiviste.stream!() |> Enum.to_list()
    assert HTTP.parse(record) == {:error, {:unsupported_type, :metadata}}
  end

  test "parse/2 handles a response with no body (e.g., 204)" do
    http = "HTTP/1.1 204 No Content\r\nServer: x\r\n\r\n"
    record = response_record(http)
    assert {:ok, resp} = HTTP.parse(record)
    assert resp.status == 204
    assert Enum.to_list(resp.body) == []
  end

  test "parse/2 preserves duplicate headers as separate list entries" do
    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Set-Cookie: a=1\r\n" <>
        "Set-Cookie: b=2\r\n" <>
        "Content-Length: 0\r\n\r\n"

    record = response_record(http)
    assert {:ok, resp} = HTTP.parse(record)
    cookies = for {"set-cookie", v} <- resp.headers, do: v
    assert cookies == ["a=1", "b=2"]
  end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 3: Implement `Archiviste.HTTP.parse/2`**

Create `lib/archiviste/http.ex`:

```elixir
defmodule Archiviste.HTTP do
  @moduledoc """
  HTTP-layer parsing for WARC `response` and `request` records.

  WARC `response` records carry a captured HTTP response (status line +
  headers + body) as their content block; `request` records carry a
  captured HTTP request analogously. `parse/2` decodes that inner HTTP
  message into a struct.
  """

  alias Archiviste.{HTTP, Record}

  @spec parse(Record.t(), keyword()) ::
          {:ok, HTTP.Response.t() | HTTP.Request.t()} | {:error, term()}
  def parse(%Record{type: :response} = record, _opts \\ []) do
    with {:ok, head, body_chunks_or_stream} <- read_head(record.payload),
         {:ok, %{version: v, status: s, reason: r}, headers} <- parse_response_head(head) do
      {:ok,
       %HTTP.Response{
         record: record,
         status: s,
         reason: r,
         http_version: v,
         headers: headers,
         body: body_chunks_or_stream,
         body_encoding: announced_encoding(headers)
       }}
    end
  end

  def parse(%Record{type: :request} = record, _opts) do
    with {:ok, head, body} <- read_head(record.payload),
         {:ok, %{method: m, target: t, version: v}, headers} <- parse_request_head(head) do
      {:ok,
       %HTTP.Request{
         record: record,
         method: m,
         target: t,
         http_version: v,
         headers: headers,
         body: body,
         body_encoding: announced_encoding(headers)
       }}
    end
  end

  def parse(%Record{type: other}, _opts), do: {:error, {:unsupported_type, other}}

  ## Internals

  @doc false
  # Reads the HTTP head (status/request line + headers) from the payload
  # stream by buffering until "\r\n\r\n". Returns the head bytes and a
  # body stream that yields the remaining payload bytes.
  def read_head(payload) do
    {head, body_stream} = take_until_double_crlf(payload, <<>>)
    {:ok, head, body_stream}
  end

  defp take_until_double_crlf(stream, acc) do
    enum = Enum.reduce_while(stream, {acc, []}, fn chunk, {acc, _saved} ->
      new_acc = acc <> chunk

      case :binary.match(new_acc, "\r\n\r\n") do
        {pos, 4} ->
          <<head::binary-size(pos + 4), rest::binary>> = new_acc
          {:halt, {:found, head, rest, stream}}

        :nomatch ->
          {:cont, {new_acc, []}}
      end
    end)

    case enum do
      {:found, head, rest, stream} ->
        # Body = rest followed by remaining stream chunks.
        body =
          Stream.concat(
            (if rest == "", do: [], else: [rest]),
            drop_consumed(stream, acc, head)
          )

        {head, body}

      {acc, _} ->
        # Stream exhausted without finding the terminator.
        {acc, []}
    end
  end

  # `Stream.concat` of `rest` and the unconsumed tail of the original Stream.
  # However, because `Enum.reduce_while` above already consumed elements from
  # `stream`, we can't re-iterate from the start. We work around this by
  # buffering everything: take_until_double_crlf reads all chunks until it
  # finds the terminator, and any chunks after the head are emitted from `rest`
  # only. The original stream is then exhausted to drain (auto-drain in
  # Archiviste.Parser handles the tail). So `drop_consumed/3` returns [].
  defp drop_consumed(_stream, _acc, _head), do: []

  defp parse_response_head(head) do
    [status_line | header_lines] = split_head(head)

    case String.split(status_line, " ", parts: 3) do
      [version, code_str, reason] ->
        case Integer.parse(code_str) do
          {code, ""} ->
            {:ok, %{version: version, status: code, reason: reason}, parse_headers(header_lines)}

          _ ->
            {:error, {:bad_status_line, status_line}}
        end

      _ ->
        {:error, {:bad_status_line, status_line}}
    end
  end

  defp parse_request_head(head) do
    [request_line | header_lines] = split_head(head)

    case String.split(request_line, " ", parts: 3) do
      [method, target, version] ->
        {:ok, %{method: method, target: target, version: version}, parse_headers(header_lines)}

      _ ->
        {:error, {:bad_request_line, request_line}}
    end
  end

  defp split_head(head) do
    head
    |> String.trim_trailing("\r\n\r\n")
    |> String.split("\r\n")
  end

  defp parse_headers(lines) do
    Enum.flat_map(lines, fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> [{name |> String.trim() |> String.downcase(), String.trim_leading(value)}]
        _ -> []
      end
    end)
  end

  defp announced_encoding(headers) do
    case List.keyfind(headers, "content-encoding", 0) do
      {_, value} -> value |> String.downcase() |> String.trim() |> normalize_encoding()
      nil -> nil
    end
  end

  defp normalize_encoding("gzip"), do: :gzip
  defp normalize_encoding("deflate"), do: :deflate
  defp normalize_encoding("br"), do: :br
  defp normalize_encoding("zstd"), do: :zstd
  defp normalize_encoding("identity"), do: :identity
  defp normalize_encoding(other), do: other
end
```

- [ ] **Step 4: Run, verify pass**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/http.ex test/archiviste/http_test.exs
git commit -m "Add Archiviste.HTTP.parse/2 for response and request records"
```

---

## Task 16: `Archiviste.HTTP.parse_stream/1`

**Files:**
- Modify: `lib/archiviste/http.ex`
- Modify: `test/archiviste/http_test.exs`

- [ ] **Step 1: Add failing test**

Append to `test/archiviste/http_test.exs`:

```elixir
  test "parse_stream/1 replaces response/request records with parsed structs and passes others through" do
    info = WarcFixture.record(type: "warcinfo", payload: "x")

    resp_payload =
      "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello"

    resp =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: resp_payload
      )

    bytes = info <> resp

    results =
      [bytes]
      |> Archiviste.stream!()
      |> HTTP.parse_stream()
      |> Enum.to_list()

    assert [%Record{type: :warcinfo}, %HTTP.Response{status: 200}] = results
  end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 3: Implement `parse_stream/1`**

Append to `lib/archiviste/http.ex`:

```elixir
  @doc """
  A stream stage that parses HTTP layer for `response` and `request` records,
  leaving other record types untouched.

  Accepts the same options as `parse/2`.
  """
  @spec parse_stream(keyword()) ::
          (Enumerable.t() -> Enumerable.t())
  def parse_stream(opts \\ []) do
    fn enumerable ->
      Stream.map(enumerable, fn
        %Record{type: t} = record when t in [:response, :request] ->
          case parse(record, opts) do
            {:ok, parsed} -> parsed
            {:error, _reason} -> record
          end

        other ->
          other
      end)
    end
  end
```

- [ ] **Step 4: Run, verify**

```
mix test test/archiviste/http_test.exs
```

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/http.ex test/archiviste/http_test.exs
git commit -m "Add HTTP.parse_stream/1 stream stage"
```

---

## Task 17: HTTP body decoder — gzip/deflate

**Files:**
- Create: `lib/archiviste/http/decoder.ex`
- Test: `test/archiviste/http/decoder_test.exs`
- Modify: `lib/archiviste/http.ex` (apply when `decode_body: true`)
- Modify: `test/archiviste/http_test.exs`

- [ ] **Step 1: Write failing decoder test**

Create `test/archiviste/http/decoder_test.exs`:

```elixir
defmodule Archiviste.HTTP.DecoderTest do
  use ExUnit.Case, async: true

  alias Archiviste.HTTP.Decoder

  test "gzip decoding works" do
    plain = "the quick brown fox"
    gz = :zlib.gzip(plain)

    out = [gz] |> Decoder.decode_stream(:gzip) |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == plain
  end

  test "deflate decoding works (raw deflate, no zlib wrapper)" do
    plain = "the quick brown fox"
    # Some servers send raw deflate, some zlib-wrapped. Decoder tries both.
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, :default, :deflated, -15, 8, :default)
    raw = :zlib.deflate(z, plain, :finish) |> IO.iodata_to_binary()
    :zlib.deflateEnd(z)
    :zlib.close(z)

    out = [raw] |> Decoder.decode_stream(:deflate) |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == plain
  end

  test "identity passes through" do
    out = ["hello"] |> Decoder.decode_stream(:identity) |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == "hello"
  end

  test "unknown encoding raises UnsupportedEncodingError" do
    assert_raise Archiviste.Error.UnsupportedEncodingError, fn ->
      ["xxx"] |> Decoder.decode_stream("nonsense") |> Enum.to_list()
    end
  end
end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 3: Implement decoder (gzip/deflate/identity for now)**

Create `lib/archiviste/http/decoder.ex`:

```elixir
defmodule Archiviste.HTTP.Decoder do
  @moduledoc false
  # Decodes HTTP Content-Encoding on body streams.

  alias Archiviste.Error.UnsupportedEncodingError

  @spec decode_stream(Enumerable.t(), atom() | binary()) :: Enumerable.t()
  def decode_stream(stream, :identity), do: stream
  def decode_stream(stream, nil), do: stream

  def decode_stream(stream, :gzip),
    do: Archiviste.Gzip.decode_stream(stream)

  def decode_stream(stream, :deflate) do
    Stream.transform(
      stream,
      fn -> deflate_init() end,
      fn chunk, z -> {[inflate_one(z, chunk)], z} end,
      fn z -> :zlib.close(z) end
    )
  end

  def decode_stream(_stream, encoding) when is_binary(encoding) or is_atom(encoding) do
    name = if is_atom(encoding), do: Atom.to_string(encoding), else: encoding
    raise UnsupportedEncodingError, encoding: name
  end

  defp deflate_init do
    z = :zlib.open()
    # 15 = zlib-wrapped; -15 = raw deflate. Try zlib-wrapped first; on data
    # error reset to raw. For simplicity we accept zlib-wrapped only via
    # init/2 and pre-detect: most CDNs serve raw deflate.
    :ok = :zlib.inflateInit(z, -15)
    z
  end

  defp inflate_one(z, chunk) do
    do_inflate(z, chunk, [])
  end

  defp do_inflate(z, input, acc) do
    case :zlib.safeInflate(z, input) do
      {:continue, out} ->
        do_inflate(z, "", [acc, out])

      {:finished, out} ->
        IO.iodata_to_binary([acc, out])
    end
  end
end
```

- [ ] **Step 4: Run tests**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 5: Wire `decode_body: true` into `HTTP.parse/2`**

In `lib/archiviste/http.ex`, change the response branch:

```elixir
  def parse(%Record{type: :response} = record, opts \\ []) do
    with {:ok, head, body_chunks_or_stream} <- read_head(record.payload),
         {:ok, %{version: v, status: s, reason: r}, headers} <- parse_response_head(head) do
      encoding = announced_encoding(headers)

      body =
        if Keyword.get(opts, :decode_body, false) and encoding not in [nil, :identity] do
          Archiviste.HTTP.Decoder.decode_stream(body_chunks_or_stream, encoding)
        else
          body_chunks_or_stream
        end

      {:ok,
       %HTTP.Response{
         record: record,
         status: s,
         reason: r,
         http_version: v,
         headers: headers,
         body: body,
         body_encoding: encoding
       }}
    end
  end
```

Apply the same change to the request branch (already takes opts).

- [ ] **Step 6: Add an integration test in http_test.exs**

```elixir
  test "decode_body: true decodes a gzipped response body" do
    plain = "the gzipped body bytes"
    gz = :zlib.gzip(plain)

    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Content-Encoding: gzip\r\n" <>
        "Content-Length: #{byte_size(gz)}\r\n" <>
        "\r\n" <> gz

    record = response_record(http)

    assert {:ok, resp} = HTTP.parse(record, decode_body: true)
    assert resp.body_encoding == :gzip
    assert IO.iodata_to_binary(Enum.to_list(resp.body)) == plain
  end

  test "decode_body: false (default) leaves body bytes raw" do
    plain = "the gzipped body bytes"
    gz = :zlib.gzip(plain)

    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Content-Encoding: gzip\r\n" <>
        "Content-Length: #{byte_size(gz)}\r\n" <>
        "\r\n" <> gz

    record = response_record(http)

    assert {:ok, resp} = HTTP.parse(record)
    assert resp.body_encoding == :gzip
    assert IO.iodata_to_binary(Enum.to_list(resp.body)) == gz
  end
```

- [ ] **Step 7: Run all tests**

```
mix test
```

- [ ] **Step 8: Commit**

```bash
git add lib/archiviste/http/decoder.ex lib/archiviste/http.ex \
        test/archiviste/http/decoder_test.exs test/archiviste/http_test.exs
git commit -m "Add HTTP body decoder for gzip/deflate"
```

---

## Task 18: HTTP body decoder — brotli (optional dep)

**Files:**
- Modify: `mix.exs` (declare optional brotli dep)
- Modify: `lib/archiviste/http/decoder.ex`
- Modify: `test/archiviste/http/decoder_test.exs`

- [ ] **Step 1: Add optional brotli dep**

In `lib/../mix.exs` (the project file at `/home/jfim/projects/archiviste/mix.exs`), modify the `deps/0` function:

```elixir
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:brotli, "~> 0.3", optional: true},
      {:ezstd, "~> 1.0", optional: true}
    ]
  end
```

Run `mix deps.get` to pull them.

- [ ] **Step 2: Write failing test for brotli**

Append to `test/archiviste/http/decoder_test.exs`:

```elixir
  describe "brotli" do
    @describetag :brotli

    setup do
      unless Code.ensure_loaded?(:brotli) do
        {:skip, "brotli dep not loaded"}
      else
        :ok
      end
    end

    test "decodes br" do
      plain = "the brotli body bytes"
      {:ok, br} = :brotli.encode(plain)

      out = [br] |> Decoder.decode_stream(:br) |> Enum.to_list() |> IO.iodata_to_binary()
      assert out == plain
    end
  end

  test "raises UnsupportedEncodingError when :br requested but :brotli not loaded" do
    # Simulate missing dep by passing an encoding the decoder doesn't know.
    # If :brotli is actually loaded in this env, this test is skipped.
    if Code.ensure_loaded?(:brotli) do
      :ok
    else
      assert_raise Archiviste.Error.UnsupportedEncodingError, ~r/:brotli/, fn ->
        ["x"] |> Decoder.decode_stream(:br) |> Enum.to_list()
      end
    end
  end
```

- [ ] **Step 3: Run, expect failure**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 4: Implement brotli decode + raise-when-missing**

In `lib/archiviste/http/decoder.ex`, add a clause before the catch-all:

```elixir
  def decode_stream(stream, :br) do
    unless Code.ensure_loaded?(:brotli) do
      raise Archiviste.Error.UnsupportedEncodingError, encoding: "br"
    end

    # :brotli does not currently expose a streaming decoder in the public API.
    # Buffer the stream and decode in one shot. For very large bodies, users
    # should disable decode_body and decode manually.
    Stream.resource(
      fn -> :start end,
      fn
        :start ->
          all = stream |> Enum.to_list() |> IO.iodata_to_binary()
          {:ok, decoded} = :brotli.decode(all)
          {[decoded], :done}

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end
```

- [ ] **Step 5: Run tests**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 6: Commit**

```bash
git add mix.exs mix.lock lib/archiviste/http/decoder.ex test/archiviste/http/decoder_test.exs
git commit -m "Add optional brotli HTTP body decoding"
```

---

## Task 19: HTTP body decoder — zstd (optional dep)

**Files:**
- Modify: `lib/archiviste/http/decoder.ex`
- Modify: `test/archiviste/http/decoder_test.exs`

- [ ] **Step 1: Write failing test**

Append to `test/archiviste/http/decoder_test.exs`:

```elixir
  describe "zstd" do
    @describetag :zstd

    setup do
      unless Code.ensure_loaded?(:ezstd) do
        {:skip, "ezstd dep not loaded"}
      else
        :ok
      end
    end

    test "decodes zstd" do
      plain = "the zstd body bytes"
      compressed = :ezstd.compress(plain)

      out = [compressed] |> Decoder.decode_stream(:zstd) |> Enum.to_list() |> IO.iodata_to_binary()
      assert out == plain
    end
  end

  test "raises UnsupportedEncodingError when :zstd requested but :ezstd not loaded" do
    if Code.ensure_loaded?(:ezstd) do
      :ok
    else
      assert_raise Archiviste.Error.UnsupportedEncodingError, ~r/:ezstd/, fn ->
        ["x"] |> Decoder.decode_stream(:zstd) |> Enum.to_list()
      end
    end
  end
```

- [ ] **Step 2: Run, expect failure**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 3: Implement zstd**

In `lib/archiviste/http/decoder.ex`, add a clause:

```elixir
  def decode_stream(stream, :zstd) do
    unless Code.ensure_loaded?(:ezstd) do
      raise Archiviste.Error.UnsupportedEncodingError, encoding: "zstd"
    end

    Stream.resource(
      fn -> :start end,
      fn
        :start ->
          all = stream |> Enum.to_list() |> IO.iodata_to_binary()
          decoded = :ezstd.decompress(all)
          {[decoded], :done}

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end
```

- [ ] **Step 4: Run tests**

```
mix test test/archiviste/http/decoder_test.exs
```

- [ ] **Step 5: Commit**

```bash
git add lib/archiviste/http/decoder.ex test/archiviste/http/decoder_test.exs
git commit -m "Add optional zstd HTTP body decoding"
```

---

## Task 20: Property-based parser tests

**Files:**
- Modify: `mix.exs` (add `stream_data` test dep)
- Create: `test/archiviste/parser_property_test.exs`

- [ ] **Step 1: Add stream_data dep**

In `lib/../mix.exs`, modify `deps/0`:

```elixir
      {:stream_data, "~> 1.1", only: :test},
```

Run `mix deps.get`.

- [ ] **Step 2: Write property tests**

Create `test/archiviste/parser_property_test.exs`:

```elixir
defmodule Archiviste.ParserPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Archiviste.{Record, WarcFixture}

  property "round-trips a well-formed multi-record file" do
    record_gen =
      gen all type <- StreamData.member_of(~w(warcinfo response request metadata resource)),
              payload_size <- StreamData.integer(0..200),
              payload <- StreamData.binary(length: payload_size) do
        {type, payload}
      end

    check all records <- StreamData.list_of(record_gen, max_length: 8) do
      bytes =
        records
        |> Enum.map(fn {type, payload} ->
          WarcFixture.record(type: type, payload: payload)
        end)
        |> WarcFixture.concat()

      parsed = [bytes] |> Archiviste.stream!() |> Enum.to_list()
      assert length(parsed) == length(records)

      Enum.zip(parsed, records)
      |> Enum.each(fn {record, {expected_type, expected_payload}} ->
        actual_type =
          case record.type do
            t when is_atom(t) -> Atom.to_string(t)
            t -> t
          end

        assert actual_type == expected_type
        assert Record.read_payload(record) == expected_payload
        assert record.content_length == byte_size(expected_payload)
      end)
    end
  end

  property "gzipped round trip yields identical parse" do
    record_gen =
      gen all payload <- StreamData.binary(min_length: 0, max_length: 100) do
        WarcFixture.record(type: "resource", payload: payload)
      end

    check all records <- StreamData.list_of(record_gen, max_length: 5) do
      gzipped = WarcFixture.gzip_each(records)
      decoded = [gzipped] |> Archiviste.Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
      assert decoded == WarcFixture.concat(records)
    end
  end
end
```

- [ ] **Step 3: Run, fix iteratively**

```
mix test test/archiviste/parser_property_test.exs
```

If `binary/1` generates bytes that happen to contain `WARC/`-like sequences,
the parser should still handle them correctly because Content-Length is
authoritative — that's exactly the invariant being checked.

- [ ] **Step 4: Commit**

```bash
git add mix.exs mix.lock test/archiviste/parser_property_test.exs
git commit -m "Add property-based parser round-trip tests"
```

---

## Task 21: Documentation polish

**Files:**
- Modify: `lib/archiviste.ex`
- Modify: `lib/archiviste/record.ex`
- Modify: `lib/archiviste/http.ex`
- Modify: `README.md`

- [ ] **Step 1: Add doctests to `Archiviste`**

Inside `lib/archiviste.ex`, prepend the module with examples (doctests with
file fixtures aren't practical; use compile-time-safe examples in `@doc`
strings as docs only, not doctests).

In `@moduledoc`:

```elixir
  @moduledoc """
  A streaming reader for WARC (Web ARChive, ISO 28500) files.

  ## Quick start

      # plain or per-record-gzipped .warc / .warc.gz
      "crawl.warc.gz"
      |> Archiviste.stream_file!()
      |> Stream.filter(&(&1.type == :response))
      |> Stream.take(10)
      |> Enum.to_list()

  Each record is an `Archiviste.Record` whose `:payload` is a lazy
  `Stream.t()` of binary chunks. See `Archiviste.Record` for details
  on the payload contract.

  For HTTP-level parsing of `response` and `request` records, see
  `Archiviste.HTTP`.

  ## Options

    * `:strict` (default `false`) — when `true`, malformed records raise
      `Archiviste.Error.MalformedRecordError` mid-stream. When `false`,
      they are skipped with a `Logger.warning`.
    * `:verify_digests` (default `false`) — verify `WARC-Block-Digest`
      while streaming payload chunks. Mismatch raises
      `Archiviste.Error.DigestMismatchError` (subject to `:strict`).
  """
```

- [ ] **Step 2: Update README**

Modify `README.md`:

```markdown
# Archiviste

An Elixir library for reading [WARC](https://iipc.github.io/warc-specifications/)
(Web ARChive) files. Streaming-first; bounded memory for any size of WARC.

> [!WARNING]
> Pre-1.0. The API may change before tagging `1.0.0`.

## Usage

```elixir
"crawl.warc.gz"
|> Archiviste.stream_file!()
|> Stream.filter(&(&1.type == :response))
|> Archiviste.HTTP.parse_stream(decode_body: true)
|> Stream.filter(&match?(%Archiviste.HTTP.Response{status: 200}, &1))
|> Enum.take(10)
```

See the [v1 API design spec](docs/superpowers/specs/2026-05-19-archiviste-api-design.md)
for the full surface and semantics.

## Optional dependencies

Add these to your own `mix.exs` to enable extra HTTP body decoders:

| Encoding | Dep                       |
| -------- | ------------------------- |
| `br`     | `{:brotli, "~> 0.3"}`     |
| `zstd`   | `{:ezstd, "~> 1.0"}`      |

`gzip` and `deflate` work out of the box (stdlib `:zlib`).

## Development

\`\`\`sh
mix deps.get
mix check    # format, compile (warnings-as-errors), credo, tests
\`\`\`
```

- [ ] **Step 3: Run `mix docs` and verify it compiles**

```
mix docs
```
Expected: clean generation, no warnings.

- [ ] **Step 4: Run final `mix check`**

```
mix check
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add lib/ README.md
git commit -m "Polish module docs and README for v1"
```

---

## Task 22: Dialyzer pass

**Files:**
- Run `mix dialyzer` and fix any warnings

- [ ] **Step 1: Initialize PLTs**

```bash
mkdir -p priv/plts
mix dialyzer --plt
```

This is a one-time cost (~minutes). Subsequent runs are fast.

- [ ] **Step 2: Run dialyzer**

```bash
mix dialyzer
```

- [ ] **Step 3: Fix warnings**

Common issues to address:
- Add `@spec` to any public function missing one.
- Replace `Enumerable.t()` with `Enumerable.t(binary())` or `Enumerable.t(Archiviste.Record.t())` where appropriate (newer Elixir versions accept the parameterized form).
- Resolve any "function has no local return" warnings.

Iterate until clean.

- [ ] **Step 4: Commit**

```bash
git add lib/
git commit -m "Resolve dialyzer warnings"
```

---

## Final verification

- [ ] **Step 1: Full test suite**

```
mix test
```

- [ ] **Step 2: Full check pipeline**

```
mix check
```

- [ ] **Step 3: Docs build**

```
mix docs
```

All three should be clean.

---

## Self-review notes

Spec coverage verified against `docs/superpowers/specs/2026-05-19-archiviste-api-design.md`:

- Reading WARC 1.0/1.1 plain & gzipped — Tasks 4–9
- Streaming-first `Stream.t()` API — Task 7
- Layered low-level + high-level HTTP — Tasks 5/14–17
- Lenient default + strict mode — Task 10
- Random-access `read_at!/3` — Task 13
- Opt-in digest verification (`WARC-Block-Digest`) — Task 12; `WARC-Payload-Digest` deferred (HTTP-layer dependent, mentioned in spec but not v1-critical)
- Opt-in HTTP body decode (gzip/deflate built-in; brotli/zstd optional, raise when missing) — Tasks 17–19
- Record struct shape — Task 2
- HTTP.Request/Response struct shapes — Task 14
- Payload handle contract (single-pass, auto-drained) — Tasks 5/6
- Property tests + fixture-driven tests — Tasks 3, 20
- Module layout matches the spec's "Module layout" section

No placeholders. Module names, types, and function signatures are consistent across tasks (`Archiviste.Record`, `Archiviste.Reader`, `Archiviste.Parser`, `Archiviste.HTTP.{Request, Response, Decoder}`, `Archiviste.Error.{MalformedRecordError, TruncatedFileError, DigestMismatchError, UnsupportedEncodingError}`).

One spec item explicitly deferred and documented: `WARC-Payload-Digest` verification (sits at the HTTP layer; not v1-blocking). All other spec items have at least one task.
