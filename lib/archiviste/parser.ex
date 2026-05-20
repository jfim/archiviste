defmodule Archiviste.Parser do
  @moduledoc false
  # Pure WARC record parser driven by an Archiviste.Reader pid.

  alias Archiviste.{Digest, Reader, Record}

  @known_types ~w(warcinfo response request metadata resource revisit conversion continuation)

  @doc """
  Read the next record from the reader.

  Returns:
    * `{:ok, %Record{}}` — record header parsed; payload is a lazy Stream
    * `:eof` — clean end of stream
    * `{:error, reason, offset}` — malformed record at byte offset
  """
  def next_record(reader_pid, opts \\ []) do
    :ok = drain_or_warn(reader_pid)
    offset = Reader.offset(reader_pid)

    case Reader.peek(reader_pid, 1) do
      :eof ->
        :eof

      {:ok, _} ->
        with {:ok, version} <- read_version_line(reader_pid, offset),
             {:ok, headers} <- read_header_block(reader_pid, offset),
             {:ok, parsed} <- interpret_headers(headers, offset) do
          payload_stream =
            build_payload_stream(reader_pid, parsed.content_length, headers, parsed.id, opts)

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

  defp drain_or_warn(reader_pid) do
    case Reader.drain_pending(reader_pid) do
      :ok -> :ok
      :eof -> :ok
    end
  end

  defp build_payload_stream(reader, content_length, headers, record_id, opts) do
    # Tell the reader how many bytes belong to the next record's payload +
    # trailing CRLF CRLF, so that if the caller doesn't consume the payload
    # we can drain it later.
    :ok = Reader.set_pending_skip(reader, content_length + 4)
    chunk_size = 64 * 1024
    verify? = Keyword.get(opts, :verify_digests, false)

    digest_ctx =
      with true <- verify?,
           {:ok, header} <- Map.fetch(headers, "warc-block-digest"),
           {:ok, algo, expected} <- Digest.algo_from_header(header) do
        {Digest.init(algo), expected, header}
      else
        _ -> nil
      end

    Stream.resource(
      fn -> {content_length, digest_ctx} end,
      fn
        {0, dctx} ->
          case Reader.read(reader, 4) do
            {:ok, _} ->
              :ok = Reader.consume_pending_skip(reader, 4)
              verify_final(dctx, record_id)

            :eof ->
              raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
          end

          {:halt, {0, nil}}

        {remaining, dctx} ->
          n = min(chunk_size, remaining)

          case Reader.read(reader, n) do
            {:ok, bytes} ->
              :ok = Reader.consume_pending_skip(reader, n)
              new_dctx = update_dctx(dctx, bytes)
              {[bytes], {remaining - n, new_dctx}}

            :eof ->
              raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
          end
      end,
      fn _ -> :ok end
    )
  end

  defp update_dctx(nil, _), do: nil

  defp update_dctx({state, expected, header}, bytes),
    do: {Digest.update(state, bytes), expected, header}

  defp verify_final(nil, _), do: :ok

  defp verify_final({state, _expected, header}, record_id) do
    actual = Digest.finalize_base32(state)
    expected_clean = String.split(header, ":", parts: 2) |> List.last() |> String.trim()

    if actual == expected_clean do
      :ok
    else
      raise Archiviste.Error.DigestMismatchError,
        record_id: record_id,
        digest_kind: :block,
        expected: header,
        actual: "sha?:" <> actual
    end
  end
end
