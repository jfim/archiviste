defmodule Archiviste.HTTPTest do
  use ExUnit.Case, async: true

  alias Archiviste.HTTP

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
end
