defmodule Archiviste.HTTPTest do
  use ExUnit.Case, async: true

  alias Archiviste.HTTP
  alias Archiviste.{Record, WarcFixture}

  test "Response struct builds with expected fields" do
    resp = %HTTP.Response{
      record: nil,
      status: 200,
      reason: "OK",
      http_version: "HTTP/1.1",
      headers: [{"content-type", "text/html"}],
      body: ["<html></html>"],
      body_encoding: nil
    }

    assert resp.status == 200
    assert resp.headers == [{"content-type", "text/html"}]
  end

  test "Request struct builds with expected fields" do
    req = %HTTP.Request{
      record: nil,
      method: "GET",
      target: "/",
      http_version: "HTTP/1.1",
      headers: [{"host", "example.com"}],
      body: [],
      body_encoding: nil
    }

    assert req.method == "GET"
    assert req.target == "/"
  end

  # Build a response record with payload eagerly buffered so that the Reader
  # GenServer can be closed before HTTP.parse/2 is called in tests.
  defp response_record(http_payload) do
    bytes =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: http_payload
      )

    [record] =
      [bytes]
      |> Archiviste.stream!()
      |> Stream.map(fn r -> %{r | payload: [Record.read_payload(r)]} end)
      |> Enum.to_list()

    record
  end

  test "parse/2 parses status line and headers of a response" do
    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Content-Type: text/html\r\n" <>
        "Content-Length: 11\r\n" <>
        "\r\n" <>
        "hello world"

    record = response_record(http)
    assert {:ok, %HTTP.Response{} = resp} = HTTP.parse(record)
    assert resp.status == 200
    assert resp.reason == "OK"
    assert resp.http_version == "HTTP/1.1"
    assert {"content-type", "text/html"} in resp.headers
    assert IO.iodata_to_binary(Enum.to_list(resp.body)) == "hello world"
  end

  test "parse/2 returns error on non-response/non-request record" do
    bytes = WarcFixture.record(type: "metadata", payload: "k: v")
    [record] = [bytes] |> Archiviste.stream!() |> Enum.to_list()
    assert HTTP.parse(record) == {:error, {:unsupported_type, :metadata}}
  end

  test "parse/2 handles a response with no body (e.g., 204)" do
    http = "HTTP/1.1 204 No Content\r\nServer: x\r\n\r\n"
    record = response_record(http)
    assert {:ok, resp} = HTTP.parse(record)
    assert resp.status == 204
    assert Enum.to_list(resp.body) == []
  end

  test "parse/2 preserves duplicate headers as separate list entries" do
    http =
      "HTTP/1.1 200 OK\r\n" <>
        "Set-Cookie: a=1\r\n" <>
        "Set-Cookie: b=2\r\n" <>
        "Content-Length: 0\r\n\r\n"

    record = response_record(http)
    assert {:ok, resp} = HTTP.parse(record)
    cookies = for {"set-cookie", v} <- resp.headers, do: v
    assert cookies == ["a=1", "b=2"]
  end

  test "parse_stream/1 replaces response/request records with parsed structs and passes others through" do
    info = WarcFixture.record(type: "warcinfo", payload: "x")

    resp_payload =
      "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello"

    resp =
      WarcFixture.record(
        type: "response",
        target_uri: "https://example.com/",
        content_type: "application/http;msgtype=response",
        payload: resp_payload
      )

    bytes = info <> resp

    results =
      [bytes]
      |> Archiviste.stream!()
      |> HTTP.parse_stream()
      |> Enum.to_list()

    assert [%Record{type: :warcinfo}, %HTTP.Response{status: 200}] = results
  end
end
