defmodule Archiviste.RecordTest do
  use ExUnit.Case, async: true

  alias Archiviste.Record

  test "struct has expected default fields" do
    record = %Record{
      version: "WARC/1.1",
      type: :response,
      id: "<urn:uuid:abc>",
      date: ~U[2026-05-19 00:00:00Z],
      target_uri: "https://example.com/",
      content_type: "application/http;msgtype=response",
      content_length: 0,
      headers: %{},
      payload: [],
      offset: 0
    }

    assert record.version == "WARC/1.1"
    assert record.type == :response
    assert record.payload == []
  end

  test "read_payload/1 concatenates a Stream of binaries" do
    record = %Record{
      version: "WARC/1.1",
      type: :response,
      id: "<id>",
      date: ~U[2026-01-01 00:00:00Z],
      target_uri: nil,
      content_type: nil,
      content_length: 5,
      headers: %{},
      payload: Stream.cycle(["he", "ll", "o"]) |> Stream.take(3),
      offset: 0
    }

    assert Record.read_payload(record) == "hello"
  end

  test "discard_payload/1 drains the payload Stream and returns :ok" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    payload =
      Stream.unfold(0, fn
        n when n < 3 ->
          Agent.update(counter, &(&1 + 1))
          {<<n>>, n + 1}

        _ ->
          nil
      end)

    record = %Record{
      version: "WARC/1.1",
      type: :metadata,
      id: "<id>",
      date: ~U[2026-01-01 00:00:00Z],
      target_uri: nil,
      content_type: nil,
      content_length: 3,
      headers: %{},
      payload: payload,
      offset: 0
    }

    assert Record.discard_payload(record) == :ok
    assert Agent.get(counter, & &1) == 3
  end
end
