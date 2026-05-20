defmodule Archiviste.LifecycleTest do
  use ExUnit.Case, async: true

  alias Archiviste.WarcFixture

  defp http_response_bytes(body) do
    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: #{byte_size(body)}\r\n\r\n" <>
      body
  end

  defp write_temp_warc(bytes, ext \\ ".warc") do
    path =
      Path.join(
        System.tmp_dir!(),
        "archiviste_lifecycle_#{System.unique_integer([:positive])}#{ext}"
      )

    File.write!(path, bytes)
    path
  end

  test "records remain consumable after outer stream is materialized (Enum.to_list)" do
    http = http_response_bytes("hello-body")

    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(
          type: "response",
          content_type: "application/http; msgtype=response",
          payload: http
        )
      ])

    path = write_temp_warc(bytes)

    try do
      records = path |> Archiviste.stream_file!() |> Enum.to_list()
      assert [warcinfo, response] = records
      assert warcinfo.type == :warcinfo
      assert response.type == :response

      # This must NOT crash with "no process":
      assert {:ok, %Archiviste.HTTP.Response{status: 200} = resp} =
               Archiviste.HTTP.parse(response)

      body_bytes = resp.body |> Enum.to_list() |> IO.iodata_to_binary()
      assert body_bytes == "hello-body"

      # The warcinfo payload should still be readable too.
      assert Archiviste.Record.read_payload(warcinfo) == "info"
    after
      File.rm(path)
    end
  end

  test "HTTP.parse_stream output remains consumable after outer stream is materialized" do
    http1 = http_response_bytes("first-body")
    http2 = http_response_bytes("second-body")

    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(
          type: "response",
          content_type: "application/http; msgtype=response",
          payload: http1
        ),
        WarcFixture.record(
          type: "response",
          content_type: "application/http; msgtype=response",
          payload: http2
        )
      ])

    path = write_temp_warc(bytes)

    try do
      parsed =
        path
        |> Archiviste.stream_file!()
        |> Archiviste.HTTP.parse_stream()
        |> Enum.to_list()

      bodies =
        Enum.flat_map(parsed, fn
          %Archiviste.HTTP.Response{body: body} ->
            [body |> Enum.to_list() |> IO.iodata_to_binary()]

          _ ->
            []
        end)

      assert bodies == ["first-body", "second-body"]
    after
      File.rm(path)
    end
  end

  test "Stream.take terminates outer stream but taken records remain consumable" do
    bytes =
      WarcFixture.concat([
        WarcFixture.record(type: "warcinfo", payload: "info"),
        WarcFixture.record(type: "response", payload: "body-1"),
        WarcFixture.record(type: "response", payload: "body-2"),
        WarcFixture.record(type: "response", payload: "body-3")
      ])

    path = write_temp_warc(bytes)

    try do
      taken =
        path
        |> Archiviste.stream_file!()
        |> Stream.filter(&(&1.type == :response))
        |> Enum.take(2)

      assert length(taken) == 2

      # Bodies must still be readable after Stream.take ended the outer stream.
      payloads = Enum.map(taken, &Archiviste.Record.read_payload/1)
      assert payloads == ["body-1", "body-2"]
    after
      File.rm(path)
    end
  end
end
