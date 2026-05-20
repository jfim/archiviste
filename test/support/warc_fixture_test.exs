defmodule Archiviste.WarcFixtureTest do
  use ExUnit.Case, async: true

  alias Archiviste.WarcFixture

  test "builds a minimal response record with correct framing" do
    bytes =
      WarcFixture.record(
        type: "response",
        id: "<urn:uuid:11111111-1111-1111-1111-111111111111>",
        date: "2026-05-19T00:00:00Z",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: "HTTP/1.1 200 OK\r\n\r\nhello"
      )

    assert String.starts_with?(bytes, "WARC/1.1\r\n")
    assert bytes =~ "WARC-Type: response\r\n"
    assert bytes =~ "Content-Length: 24\r\n"
    assert String.ends_with?(bytes, "\r\nhello\r\n\r\n")
  end

  test "concat/1 joins multiple records" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "b")
    assert WarcFixture.concat([a, b]) == a <> b
  end

  test "gzip/1 wraps bytes in a single gzip member" do
    bytes = WarcFixture.record(type: "warcinfo", payload: "hello")
    gzipped = WarcFixture.gzip(bytes)
    assert <<0x1F, 0x8B, _::binary>> = gzipped
    assert :zlib.gunzip(gzipped) == bytes
  end
end
