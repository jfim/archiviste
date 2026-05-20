defmodule Archiviste.HTTP.Decoder do
  @moduledoc false
  # Decodes HTTP Content-Encoding on body streams.

  alias Archiviste.Error.UnsupportedEncodingError

  @spec decode_stream(Enumerable.t(), atom() | binary() | nil) :: Enumerable.t()
  def decode_stream(stream, nil), do: stream
  def decode_stream(stream, :identity), do: stream

  def decode_stream(stream, :gzip),
    do: Archiviste.Gzip.decode_stream(stream)

  def decode_stream(stream, :deflate) do
    Stream.transform(
      stream,
      fn -> deflate_init() end,
      fn chunk, z -> {[inflate_one(z, chunk)], z} end,
      fn z -> :zlib.close(z) end
    )
  end

  def decode_stream(stream, :br) do
    unless Code.ensure_loaded?(:brotli) do
      raise UnsupportedEncodingError, encoding: "br"
    end

    # :brotli does not expose a streaming decoder in its public API.
    # Buffer the stream and decode in one shot.
    # Use apply/3 to avoid compile-time warnings when :brotli is not present.
    Stream.resource(
      fn -> :start end,
      fn
        :start ->
          all = stream |> Enum.to_list() |> IO.iodata_to_binary()
          # credo:disable-for-next-line Credo.Check.Refactor.Apply
          {:ok, decoded} = apply(:brotli, :decode, [all])
          {[decoded], :done}

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end

  def decode_stream(stream, :zstd) do
    unless Code.ensure_loaded?(:ezstd) do
      raise UnsupportedEncodingError, encoding: "zstd"
    end

    # Use apply/3 to avoid compile-time warnings when :ezstd is not present.
    Stream.resource(
      fn -> :start end,
      fn
        :start ->
          all = stream |> Enum.to_list() |> IO.iodata_to_binary()
          # credo:disable-for-next-line Credo.Check.Refactor.Apply
          decoded = apply(:ezstd, :decompress, [all])
          {[decoded], :done}

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end

  def decode_stream(_stream, encoding) when is_binary(encoding) or is_atom(encoding) do
    name = if is_atom(encoding), do: Atom.to_string(encoding), else: encoding
    raise UnsupportedEncodingError, encoding: name
  end

  defp deflate_init do
    z = :zlib.open()
    # -15 = raw deflate (no zlib wrapper); most CDNs send raw deflate
    :ok = :zlib.inflateInit(z, -15)
    z
  end

  defp inflate_one(z, chunk) do
    do_inflate(z, chunk, [])
  end

  defp do_inflate(z, input, acc) do
    case :zlib.safeInflate(z, input) do
      {:continue, out} ->
        do_inflate(z, "", [acc, out])

      {:finished, out} ->
        IO.iodata_to_binary([acc, out])
    end
  end
end
