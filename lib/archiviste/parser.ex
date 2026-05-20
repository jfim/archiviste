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
             {:ok, parsed} <- interpret_headers(headers, offset),
             {:ok, payload_stream} <-
               build_payload_stream(
                 reader_pid,
                 parsed.content_length,
                 headers,
                 parsed.id,
                 opts,
                 offset
               ) do
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

  # When verify_digests is true and the record has a WARC-Block-Digest header,
  # eagerly read and buffer the entire payload to verify the digest before
  # yielding the record. This means opting into verify_digests: true buffers
  # each verified record's payload into memory (you can't verify a digest
  # without seeing all the bytes).
  #
  # On match  → returns {:ok, replay_stream}
  # On mismatch → returns {:error, {:digest_mismatch, ...}, offset} so the
  #               caller can route through the normal lenient/strict logic.
  defp build_payload_stream(reader, content_length, headers, record_id, opts, record_offset) do
    :ok = Reader.set_pending_skip(reader, content_length + 4)
    verify? = Keyword.get(opts, :verify_digests, false)

    digest_header =
      if verify?, do: Map.get(headers, "warc-block-digest"), else: nil

    if verify? and not is_nil(digest_header) do
      # Eagerly read payload, verify digest, return replay stream or error.
      eager_verify_payload(reader, content_length, digest_header, record_id, record_offset)
    else
      # Eagerly read payload into memory and return a replay stream. Records
      # yielded by `Archiviste.stream!/2` may outlive the underlying Reader
      # (e.g., when consumers use `Enum.to_list/1` or `Stream.take/2` and
      # then iterate payloads later), so the payload must be self-contained
      # rather than a live GenServer-backed cursor.
      #
      # Tradeoff: this buffers each record's payload in memory. The
      # historical "bounded memory across a single huge record" promise is
      # weakened — bounded memory now means "across records, not across a
      # single huge record". Future work: spill payloads larger than a
      # threshold to a temp file.
      eager_payload(reader, content_length)
    end
  end

  defp eager_payload(reader, content_length) do
    case read_all_payload(reader, content_length) do
      {:ok, payload_bytes} ->
        case Reader.read(reader, 4) do
          {:ok, _} ->
            :ok = Reader.consume_pending_skip(reader, content_length + 4)
            {:ok, replay_stream(payload_bytes)}

          :eof ->
            raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
        end

      {:eof, partial} ->
        :ok = Reader.consume_pending_skip(reader, byte_size(partial))
        raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
    end
  end

  defp eager_verify_payload(reader, content_length, digest_header, record_id, record_offset) do
    case read_all_payload(reader, content_length) do
      {:ok, payload_bytes} ->
        eager_verify_trailer(
          reader,
          content_length,
          digest_header,
          record_id,
          record_offset,
          payload_bytes
        )

      {:eof, partial} ->
        # Truncated: consume what we have so the offset is accurate, then raise.
        :ok = Reader.consume_pending_skip(reader, byte_size(partial))
        raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
    end
  end

  defp eager_verify_trailer(
         reader,
         content_length,
         digest_header,
         record_id,
         record_offset,
         payload_bytes
       ) do
    case Reader.read(reader, 4) do
      {:ok, _} ->
        :ok = Reader.consume_pending_skip(reader, content_length + 4)
        check_digest(digest_header, payload_bytes, record_id, record_offset)

      :eof ->
        raise Archiviste.Error.TruncatedFileError, offset: Reader.offset(reader)
    end
  end

  defp check_digest(digest_header, payload_bytes, record_id, record_offset) do
    case Digest.verify(digest_header, payload_bytes) do
      :ok ->
        {:ok, replay_stream(payload_bytes)}

      {:error, :mismatch, expected_header, actual_encoded} ->
        {:error, {:digest_mismatch, expected_header, actual_encoded, record_id}, record_offset}

      {:error, :unknown_algorithm, _} ->
        # Unknown algorithm — skip verification, return replay stream as-is.
        {:ok, replay_stream(payload_bytes)}
    end
  end

  defp replay_stream(payload_bytes) do
    Stream.resource(fn -> payload_bytes end, &replay_step/1, fn _ -> :ok end)
  end

  defp read_all_payload(reader, content_length) do
    read_all_payload(reader, content_length, <<>>)
  end

  defp read_all_payload(_reader, 0, acc), do: {:ok, acc}

  defp read_all_payload(reader, remaining, acc) do
    chunk_size = min(64 * 1024, remaining)

    case Reader.read(reader, chunk_size) do
      {:ok, bytes} ->
        :ok = Reader.consume_pending_skip(reader, chunk_size)
        read_all_payload(reader, remaining - chunk_size, acc <> bytes)

      :eof ->
        {:eof, acc}
    end
  end

  defp replay_step(<<>>), do: {:halt, <<>>}
  defp replay_step(bytes), do: {[bytes], <<>>}
end
