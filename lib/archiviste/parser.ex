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
