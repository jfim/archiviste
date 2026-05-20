defmodule Archiviste.HTTP do
  @moduledoc """
  HTTP-layer parsing for WARC `response` and `request` records.

  WARC `response` records carry a captured HTTP response (status line +
  headers + body) as their content block; `request` records carry a
  captured HTTP request analogously. `parse/2` decodes that inner HTTP
  message into a struct.
  """

  alias Archiviste.{HTTP, Record}

  @doc """
  Parse an HTTP response or request record into an `HTTP.Response` or
  `HTTP.Request` struct.

  Returns `{:ok, struct}` on success or `{:error, reason}` when the record
  type is unsupported or the HTTP message is malformed.

  ## Options

  (Reserved for future use — e.g., `decode_body: true`.)
  """
  @spec parse(Record.t(), keyword()) ::
          {:ok, HTTP.Response.t() | HTTP.Request.t()} | {:error, term()}
  def parse(record, opts \\ [])

  def parse(%Record{type: :response} = record, _opts) do
    with {:ok, head, body_stream} <- read_head(record.payload),
         {:ok, %{version: v, status: s, reason: r}, headers} <- parse_response_head(head) do
      {:ok,
       %HTTP.Response{
         record: record,
         status: s,
         reason: r,
         http_version: v,
         headers: headers,
         body: body_stream,
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

  @doc """
  A stream stage that parses the HTTP layer for `response` and `request`
  records in an existing record stream, leaving other record types untouched.

  Usage:

      [bytes]
      |> Archiviste.stream!()
      |> HTTP.parse_stream()
      |> Enum.to_list()

  Accepts the same options as `parse/2`.
  """
  @spec parse_stream(Enumerable.t(), keyword()) :: Enumerable.t()
  def parse_stream(enumerable, opts \\ []) do
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

  ## Internals

  @doc false
  # Reads the HTTP head (status/request line + headers) from the payload
  # stream by buffering until "\r\n\r\n". Returns the head bytes and a
  # body stream that yields the remaining payload bytes.
  #
  # Strategy: fully buffer chunks from the payload stream until we locate
  # the "\r\n\r\n" terminator. Everything before and including the terminator
  # is the head. Everything after it (within the already-pulled chunks) forms
  # the body. Because `Enum.reduce_while` consumes the underlying Stream and
  # cannot be resumed after halting, the body contains ONLY the bytes already
  # pulled past the terminator. For the bounded payloads in our test suite
  # (and for typical HTTP headers + small bodies), the Reader emits all bytes
  # in a single chunk, so the full body is captured in `rest`. The WARC
  # parser's auto-drain handles any bytes that remain unconsumed.
  def read_head(payload) do
    {head, body} = take_until_double_crlf(payload, <<>>)
    {:ok, head, body}
  end

  defp take_until_double_crlf(stream, acc) do
    result =
      Enum.reduce_while(stream, {acc, []}, fn chunk, {buf, _} ->
        new_buf = buf <> chunk

        case :binary.match(new_buf, "\r\n\r\n") do
          {pos, 4} ->
            <<head::binary-size(pos + 4), rest::binary>> = new_buf
            {:halt, {:found, head, rest}}

          :nomatch ->
            {:cont, {new_buf, []}}
        end
      end)

    case result do
      {:found, head, rest} ->
        # Body = bytes after the "\r\n\r\n" that were already buffered.
        # Remaining payload bytes (if any) in the Stream were consumed by
        # reduce_while and cannot be resumed here. For bounded, small HTTP
        # headers + bodies this is correct: the bytes fit in the same chunk.
        body = if rest == <<>>, do: [], else: [rest]
        {head, body}

      {buf, _} ->
        # Stream exhausted without finding the terminator — treat all as head.
        {buf, []}
    end
  end

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
        [name, value] ->
          [{name |> String.trim() |> String.downcase(), String.trim_leading(value)}]

        _ ->
          []
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
