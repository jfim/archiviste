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
