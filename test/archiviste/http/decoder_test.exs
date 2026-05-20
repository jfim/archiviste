defmodule Archiviste.HTTP.DecoderTest do
  use ExUnit.Case, async: true

  alias Archiviste.HTTP.Decoder

  test "gzip decoding works" do
    plain = "the quick brown fox"
    gz = :zlib.gzip(plain)

    out = [gz] |> Decoder.decode_stream(:gzip) |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == plain
  end

  test "deflate decoding works (raw deflate, no zlib wrapper)" do
    plain = "the quick brown fox"
    # Some servers send raw deflate, some zlib-wrapped. Decoder handles raw deflate.
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, :default, :deflated, -15, 8, :default)
    raw = :zlib.deflate(z, plain, :finish) |> IO.iodata_to_binary()
    :zlib.deflateEnd(z)
    :zlib.close(z)

    out = [raw] |> Decoder.decode_stream(:deflate) |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == plain
  end

  test "identity passes through" do
    out =
      ["hello"] |> Decoder.decode_stream(:identity) |> Enum.to_list() |> IO.iodata_to_binary()

    assert out == "hello"
  end

  test "unknown encoding raises UnsupportedEncodingError" do
    assert_raise Archiviste.Error.UnsupportedEncodingError, fn ->
      ["xxx"] |> Decoder.decode_stream("nonsense") |> Enum.to_list()
    end
  end

  describe "brotli" do
    @describetag :brotli
    @describetag skip: not Code.ensure_loaded?(:brotli)

    test "decodes br" do
      plain = "the brotli body bytes"
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      {:ok, br} = apply(:brotli, :encode, [plain])

      out = [br] |> Decoder.decode_stream(:br) |> Enum.to_list() |> IO.iodata_to_binary()
      assert out == plain
    end
  end

  test "raises UnsupportedEncodingError when :br requested but :brotli not loaded" do
    if Code.ensure_loaded?(:brotli) do
      # Library is present — :br decoding works, nothing to assert here
      :ok
    else
      assert_raise Archiviste.Error.UnsupportedEncodingError, ~r/br/, fn ->
        ["x"] |> Decoder.decode_stream(:br) |> Enum.to_list()
      end
    end
  end

  describe "zstd" do
    @describetag :zstd
    @describetag skip: not Code.ensure_loaded?(:ezstd)

    test "decodes zstd" do
      plain = "the zstd body bytes"
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      compressed = apply(:ezstd, :compress, [plain])

      out =
        [compressed] |> Decoder.decode_stream(:zstd) |> Enum.to_list() |> IO.iodata_to_binary()

      assert out == plain
    end
  end

  test "raises UnsupportedEncodingError when :zstd requested but :ezstd not loaded" do
    if Code.ensure_loaded?(:ezstd) do
      # Library is present — :zstd decoding works, nothing to assert here
      :ok
    else
      assert_raise Archiviste.Error.UnsupportedEncodingError, ~r/zstd/, fn ->
        ["x"] |> Decoder.decode_stream(:zstd) |> Enum.to_list()
      end
    end
  end
end
