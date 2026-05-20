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
    with {:ok, head, body_stream} <- read_head_and_body_stream(record.payload),
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
    with {:ok, head, body} <- read_head_and_body_stream(record.payload),
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

  # Reads the HTTP head (status/request line + headers) from the payload
  # stream using Enumerable continuation semantics so the body stream can
  # resume the same underlying enumerator. This correctly handles HTTP bodies
  # that span multiple Reader chunks (>~64 KB).
  defp read_head_and_body_stream(payload) do
    reducer = &Enumerable.reduce(payload, &1, fn x, _ -> {:suspend, x} end)
    pull_until_terminator(reducer.({:cont, nil}), <<>>)
  end

  defp pull_until_terminator({:suspended, chunk, next_cont}, acc) do
    new_acc = acc <> chunk

    case :binary.match(new_acc, "\r\n\r\n") do
      {pos, 4} ->
        <<head::binary-size(pos + 4), rest::binary>> = new_acc
        body = build_body_stream(rest, next_cont)
        {:ok, head, body}

      :nomatch ->
        pull_until_terminator(next_cont.({:cont, nil}), new_acc)
    end
  end

  defp pull_until_terminator({:done, _}, _acc), do: {:error, :no_http_terminator}
  defp pull_until_terminator({:halted, _}, _acc), do: {:error, :no_http_terminator}

  defp build_body_stream(initial_leftover, cont) do
    Stream.resource(
      fn -> {initial_leftover, cont} end,
      fn
        {leftover, c} when leftover != "" ->
          {[leftover], {"", c}}

        {"", c} ->
          case c.({:cont, nil}) do
            {:suspended, chunk, next} -> {[chunk], {"", next}}
            {:done, _} -> {:halt, nil}
            {:halted, _} -> {:halt, nil}
          end
      end,
      fn _ -> :ok end
    )
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
