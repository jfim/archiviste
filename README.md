# Archiviste

An Elixir library for reading and writing [WARC](https://iipc.github.io/warc-specifications/) (Web ARChive) files.

> [!WARNING]
> Pre-alpha. The API is being designed and will change.

## Development

```sh
mix deps.get
mix check    # format, compile (warnings-as-errors), credo, tests
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `archiviste` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:archiviste, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/archiviste>.

