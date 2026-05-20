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
        case Parser.next_record(reader, opts) do
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

  @doc """
  Read exactly one record starting at a given byte offset in `path`.

  Works on both plain `.warc` files and per-record-gzipped `.warc.gz` files.
  For `.warc.gz`, the offset must point to the start of a gzip member
  (record-aligned).
  """
  @spec read_at!(Path.t(), non_neg_integer(), opts()) :: Archiviste.Record.t()
  def read_at!(path, offset, opts \\ []) when is_binary(path) and offset >= 0 do
    {:ok, io} = File.open(path, [:read, :binary])
    {:ok, _} = :file.position(io, offset)

    stream =
      Stream.resource(
        fn -> io end,
        fn io ->
          case IO.binread(io, 64 * 1024) do
            :eof -> {:halt, io}
            data when is_binary(data) -> {[data], io}
          end
        end,
        fn io -> File.close(io) end
      )

    decoded =
      if gzip?(path), do: Archiviste.Gzip.decode_stream(stream), else: stream

    # Eagerly read the payload *inside* the stream pipeline so the Reader
    # GenServer is still alive when the inner payload stream is consumed.
    # Callers wanting laziness should use stream_file!/2 with their own
    # filtering — read_at!/3 is the random-access primitive.
    result =
      decoded
      |> stream!(opts)
      |> Stream.map(fn record ->
        payload_bytes = Archiviste.Record.read_payload(record)
        %{record | payload: [payload_bytes]}
      end)
      |> Enum.take(1)

    case result do
      [record] -> record
      [] -> raise Archiviste.Error.TruncatedFileError, offset: offset
    end
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
