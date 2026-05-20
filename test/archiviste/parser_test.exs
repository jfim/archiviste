defmodule Archiviste.ParserTest do
  use ExUnit.Case, async: true

  alias Archiviste.{Parser, Reader, Record, WarcFixture}

  defp reader_from(binary), do: Reader.start_link([binary])

  test "parses a single minimal warcinfo record" do
    bytes =
      WarcFixture.record(
        type: "warcinfo",
        id: "<urn:uuid:11111111-1111-1111-1111-111111111111>",
        date: "2026-05-19T00:00:00Z",
        payload: "software: archiviste/0.1\r\n"
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, %Record{} = record} = Parser.next_record(r)
    assert record.version == "WARC/1.1"
    assert record.type == :warcinfo
    assert record.id == "<urn:uuid:11111111-1111-1111-1111-111111111111>"
    assert record.date == ~U[2026-05-19 00:00:00Z]
    assert record.content_length == 26
    assert record.offset == 0
    assert Record.read_payload(record) == "software: archiviste/0.1\r\n"
    assert Parser.next_record(r) == :eof
    Reader.close(r)
  end

  test "parses target_uri and content_type when present" do
    bytes =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: "HTTP/1.1 200 OK\r\n\r\n"
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.type == :response
    assert record.target_uri == "https://example.com/"
    assert record.content_type == "application/http;msgtype=response"
    Reader.close(r)
  end

  test "preserves all headers in the headers map (lowercased keys)" do
    bytes =
      WarcFixture.record(
        type: "response",
        headers: [{"WARC-IP-Address", "203.0.113.1"}],
        payload: ""
      )

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.headers["warc-ip-address"] == "203.0.113.1"
    assert record.headers["warc-type"] == "response"
    Reader.close(r)
  end

  test "unknown WARC-Type is preserved as a string" do
    bytes = WarcFixture.record(type: "future-type", payload: "")

    {:ok, r} = reader_from(bytes)
    assert {:ok, record} = Parser.next_record(r)
    assert record.type == "future-type"
    Reader.close(r)
  end

  test "payload Stream yields bytes lazily" do
    bytes = WarcFixture.record(type: "resource", payload: "abcdefghij")
    {:ok, r} = reader_from(bytes)
    {:ok, record} = Parser.next_record(r)
    chunks = Enum.to_list(record.payload)
    assert IO.iodata_to_binary(chunks) == "abcdefghij"
    Reader.close(r)
  end

  test "next_record/1 advances past an unconsumed payload of the previous record" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "first"),
        WarcFixture.record(type: "response", payload: "second")
      ])

    {:ok, r} = reader_from(bytes)
    {:ok, first} = Parser.next_record(r)
    # Intentionally do NOT consume `first.payload`.
    {:ok, second} = Parser.next_record(r)
    assert first.type == :warcinfo
    assert second.type == :response
    assert Record.read_payload(second) == "second"
    assert Parser.next_record(r) == :eof
    Reader.close(r)
  end

  test "next_record/1 advances past a partially consumed payload" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "resource", payload: "aaaaabbbbb"),
        WarcFixture.record(type: "resource", payload: "second")
      ])

    {:ok, r} = reader_from(bytes)
    {:ok, first} = Parser.next_record(r)
    # Consume only the first 5 bytes by taking 1 chunk; chunk_size is 64 KB
    # so this fully consumes the small payload — instead, consume nothing
    # but advance two records.
    [_chunk | _] = first.payload |> Enum.take(1)
    {:ok, second} = Parser.next_record(r)
    assert second.type == :resource
    assert Record.read_payload(second) == "second"
    Reader.close(r)
  end
end
