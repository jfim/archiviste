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

  alias Archiviste.{Parser, Reader, Record}

  @type opts :: [strict: boolean(), verify_digests: boolean()]

  @doc """
  Stream WARC records from an arbitrary enumerable of binary chunks.

  This is the core API. For files, see `stream_file!/2`.

  Each yielded `Archiviste.Record`'s `:payload` is a buffered stream whose
  bytes have already been read from the underlying source. This means payloads
  remain readable even after the outer record stream is exhausted.

  ## Options

    * `:strict` (default `false`) — when `true`, malformed records raise
      mid-stream instead of being skipped with a `Logger.warning`.
    * `:verify_digests` (default `false`) — when `true`, verify WARC block
      and payload digests; mismatches are treated as malformed records.
  """
  @spec stream!(Enumerable.t(), opts()) :: Enumerable.t()
  def stream!(enumerable, _opts \\ []) do
    Stream.resource(
      fn ->
        {:ok, reader} = Reader.start_link(enumerable)
        reader
      end,
      fn reader ->
        case Parser.next_record(reader) do
          {:ok, %Record{} = record} ->
            # Eagerly buffer the payload so it remains readable after the
            # outer stream (and its Reader process) have been closed.
            buffered = record.payload |> Enum.to_list() |> IO.iodata_to_binary()
            record = %Record{record | payload: [buffered]}
            {[record], reader}

          :eof ->
            {:halt, reader}

          {:error, reason, offset} ->
            raise Archiviste.Error.MalformedRecordError,
              offset: offset,
              reason: reason
        end
      end,
      fn reader -> Reader.close(reader) end
    )
  end
end
