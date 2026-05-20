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
