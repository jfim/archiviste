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

```sh
mix deps.get
mix check    # format, compile (warnings-as-errors), credo, tests
```
