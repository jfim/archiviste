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
