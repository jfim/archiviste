defmodule Archiviste do
  @moduledoc """
  A streaming reader for WARC (Web ARChive, ISO 28500) files.

  ## Quick start

      "crawl.warc.gz"
      |> Archiviste.stream_file!()
      |> Stream.filter(&(&1.type == :response))
      |> Enum.take(10)

  Each yielded record is an `Archiviste.Record` whose `:payload` is a lazy
  `Stream.t()` of binary chunks. See `Archiviste.Record` for details.
  """

  require Logger

  alias Archiviste.{Error, Parser, Reader, Record}

  @type opts :: [strict: boolean(), verify_digests: boolean()]

  @doc """
  Stream WARC records from an arbitrary enumerable of binary chunks.

  This is the core API. For files, see `stream_file!/2`.

  Each yielded `Archiviste.Record`'s `:payload` is a lazy `Stream.t()` of
  binary chunks. Its lifetime is bounded by this outer stream — consume
  payloads inside the pipeline (e.g., with `Stream.map`) before exhausting
  the outer stream with `Enum.to_list/1` or similar.

  ## Options

    * `:strict` (default `false`) — when `true`, malformed records raise
      mid-stream instead of being skipped with a `Logger.warning`.
    * `:verify_digests` (default `false`) — when `true`, verify WARC block
      and payload digests; mismatches are treated as malformed records.
  """
  @spec stream!(Enumerable.t(), opts()) :: Enumerable.t()
  def stream!(enumerable, opts \\ []) do
    strict? = Keyword.get(opts, :strict, false)

    Stream.resource(
      fn ->
        {:ok, reader} = Reader.start_link(enumerable)
        {reader, strict?}
      end,
      fn {reader, strict?} = acc ->
        case Parser.next_record(reader) do
          {:ok, %Record{} = record} ->
            {[record], acc}

          :eof ->
            {:halt, acc}

          {:error, reason, offset} when strict? ->
            raise Error.MalformedRecordError,
              offset: offset,
              reason: reason

          {:error, reason, offset} ->
            resync_lenient(reader, acc, reason, offset)
        end
      end,
      fn {reader, _} -> Reader.close(reader) end
    )
  end

  @doc """
  Stream WARC records from a file path.

  Detects per-record gzip compression from the `.gz` extension or from the
  gzip magic bytes at the start of the file.

  Accepts the same options as `stream!/2`.
  """
  @spec stream_file!(Path.t(), opts()) :: Enumerable.t()
  def stream_file!(path, opts \\ []) when is_binary(path) do
    raw = File.stream!(path, [], 64 * 1024)

    raw
    |> maybe_gunzip(path)
    |> stream!(opts)
  end

  defp resync_lenient(reader, acc, reason, offset) do
    Logger.warning("Archiviste: skipped malformed record at offset #{offset}: #{inspect(reason)}")

    :ok = Reader.clear_pending(reader)

    case Reader.scan_to(reader, "WARC/") do
      :ok -> {[], acc}
      :eof -> {:halt, acc}
    end
  end

  defp maybe_gunzip(stream, path) do
    if gzip?(path), do: Archiviste.Gzip.decode_stream(stream), else: stream
  end

  defp gzip?(path) do
    String.ends_with?(path, ".gz") or
      match?({:ok, <<0x1F, 0x8B>>}, File.open(path, [:read, :binary], &IO.binread(&1, 2)))
  end
end
