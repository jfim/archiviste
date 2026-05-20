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
