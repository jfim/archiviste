# Archiviste

An Elixir library for reading [WARC](https://iipc.github.io/warc-specifications/)
(Web ARChive) files. Streaming-first; bounded memory for any size of WARC.

> [!WARNING]
> Pre-1.0. The API may change before tagging `1.0.0`.

## Usage

Take the first 10 successful HTTP responses from a WARC:

```elixir
"crawl.warc.gz"
|> Archiviste.stream_file!()
|> Stream.filter(&(&1.type == :response))
|> Archiviste.HTTP.parse_stream(decode_body: true)
|> Stream.filter(&match?(%Archiviste.HTTP.Response{status: 200}, &1))
|> Enum.take(10)
```

Count responses grouped by HTTP status code:

```elixir
"crawl.warc.gz"
|> Archiviste.stream_file!()
|> Stream.filter(&(&1.type == :response))
|> Archiviste.HTTP.parse_stream()
|> Stream.filter(&match?(%Archiviste.HTTP.Response{}, &1))
|> Enum.reduce(%{}, fn %Archiviste.HTTP.Response{status: status}, acc ->
  Map.update(acc, status, 1, &(&1 + 1))
end)
# => %{200 => 1842, 301 => 57, 404 => 12, 500 => 3}
```

Because everything is a `Stream`, both examples run in bounded memory
regardless of WARC size.

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
