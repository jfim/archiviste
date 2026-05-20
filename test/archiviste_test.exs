defmodule ArchivisteTest do
  use ExUnit.Case, async: true
  doctest Archiviste

  alias Archiviste.{Record, WarcFixture}

  test "stream!/2 yields all records lazily" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "a"),
        WarcFixture.record(type: "response", payload: "b"),
        WarcFixture.record(type: "metadata", payload: "c")
      ])

    records = bytes |> List.wrap() |> Archiviste.stream!() |> Enum.to_list()
    types = Enum.map(records, & &1.type)
    assert types == [:warcinfo, :response, :metadata]
  end

  test "stream!/2 accepts an enumerable of chunked binaries" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "hello"),
        WarcFixture.record(type: "response", payload: "world")
      ])

    # Split into 7-byte chunks; the last chunk may be shorter.
    chunks =
      bytes
      |> :binary.bin_to_list()
      |> Enum.chunk_every(7)
      |> Enum.map(&IO.iodata_to_binary([&1]))

    records = chunks |> Archiviste.stream!() |> Enum.to_list()
    assert Enum.map(records, & &1.type) == [:warcinfo, :response]
    assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["hello", "world"]
  end

  test "stream!/2 supports Stream.filter without touching payloads" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(type: "response", payload: "body")
      ])

    [resp] =
      [bytes]
      |> Archiviste.stream!()
      |> Stream.filter(&(&1.type == :response))
      |> Enum.to_list()

    assert Record.read_payload(resp) == "body"
  end

  test "stream_file!/2 reads from a plain .warc file" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(type: "response", payload: "body")
      ])

    path =
      Path.join(System.tmp_dir!(), "archiviste_test_#{System.unique_integer([:positive])}.warc")

    File.write!(path, bytes)

    try do
      records = path |> Archiviste.stream_file!() |> Enum.to_list()
      assert Enum.map(records, & &1.type) == [:warcinfo, :response]
    after
      File.rm(path)
    end
  end
end
