defmodule Archiviste.Gzip do
  @moduledoc false
  # Streaming gunzip that handles concatenated gzip members.
  #
  # `.warc.gz` files are made of one gzip member per WARC record. We can't
  # use `:zlib.gunzip/1` (one-shot) and must instead drive `:zlib.inflate`
  # in streaming mode, detecting member boundaries (`:stream_end`) and
  # resetting the inflator for the next member.
  #
  # We use `:zlib.inflateInit/3` with the `:reset` EoS behaviour, which
  # automatically resets the inflator when a member boundary is reached and
  # keeps processing any remaining bytes as the next member.

  @spec decode_stream(Enumerable.t()) :: Enumerable.t()
  def decode_stream(enumerable) do
    Stream.transform(
      enumerable,
      fn -> new_inflator() end,
      fn chunk, z -> {inflate_chunk(z, chunk), z} end,
      fn z -> :zlib.close(z) end
    )
  end

  defp new_inflator do
    z = :zlib.open()
    # 31 = max window bits (15) + gzip flag (16)
    # :reset EoS behaviour: automatically reset and continue across gzip members.
    :ok = :zlib.inflateInit(z, 31, :reset)
    z
  end

  defp inflate_chunk(_z, ""), do: []

  defp inflate_chunk(z, chunk) do
    case :zlib.safeInflate(z, chunk) do
      {:continue, out} ->
        out_bin = IO.iodata_to_binary(out)
        # Pull any remaining output for this chunk by continuing with empty input.
        (out_bin <> drain_continue(z))
        |> wrap()

      {:finished, out} ->
        out_bin = IO.iodata_to_binary(out)
        [out_bin]
    end
  end

  defp drain_continue(z) do
    case :zlib.safeInflate(z, "") do
      {:continue, out} -> IO.iodata_to_binary(out) <> drain_continue(z)
      {:finished, out} -> IO.iodata_to_binary(out)
    end
  end

  defp wrap(bin) when bin == "", do: []
  defp wrap(bin), do: [bin]
end
