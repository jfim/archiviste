defmodule Archiviste.GzipTest do
  use ExUnit.Case, async: true

  alias Archiviste.{Gzip, WarcFixture}

  test "decodes a single gzip member" do
    bytes = WarcFixture.gzip("hello")
    out = bytes |> List.wrap() |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == "hello"
  end

  test "decodes concatenated gzip members (per-record .warc.gz layout)" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "one"),
        WarcFixture.record(type: "response", payload: "two")
      ])

    expected =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "one"),
        WarcFixture.record(type: "response", payload: "two")
      ])

    out =
      members |> List.wrap() |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()

    assert out == expected
  end

  test "decodes across chunk boundaries that split mid-member" do
    bytes = WarcFixture.gzip(String.duplicate("xyz", 5_000))
    # Split into 17-byte chunks so the gzip header/body are fragmented.
    chunks =
      bytes
      |> :binary.bin_to_list()
      |> Enum.chunk_every(17)
      |> Enum.map(&IO.iodata_to_binary([&1]))

    out = chunks |> Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()
    assert out == String.duplicate("xyz", 5_000)
  end
end
