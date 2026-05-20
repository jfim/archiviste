defmodule Archiviste.ParserPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Archiviste.{Record, WarcFixture}

  property "round-trips a well-formed multi-record file" do
    record_gen =
      gen all(
            type <- StreamData.member_of(~w(warcinfo response request metadata resource)),
            payload_size <- StreamData.integer(0..200),
            payload <- StreamData.binary(length: payload_size)
          ) do
        {type, payload}
      end

    check all(records <- StreamData.list_of(record_gen, max_length: 8)) do
      bytes =
        records
        |> Enum.map(fn {type, payload} ->
          WarcFixture.record(type: type, payload: payload)
        end)
        |> WarcFixture.concat()

      parsed =
        [bytes]
        |> Archiviste.stream!()
        |> Stream.map(fn r -> %{r | payload: [Record.read_payload(r)]} end)
        |> Enum.to_list()

      assert length(parsed) == length(records)

      Enum.zip(parsed, records)
      |> Enum.each(fn {record, {expected_type, expected_payload}} ->
        actual_type =
          case record.type do
            t when is_atom(t) -> Atom.to_string(t)
            t -> t
          end

        assert actual_type == expected_type
        assert IO.iodata_to_binary(record.payload) == expected_payload
        assert record.content_length == byte_size(expected_payload)
      end)
    end
  end

  property "gzipped round trip yields identical parse" do
    record_gen =
      gen all(payload <- StreamData.binary(min_length: 0, max_length: 100)) do
        WarcFixture.record(type: "resource", payload: payload)
      end

    check all(records <- StreamData.list_of(record_gen, max_length: 5)) do
      gzipped = WarcFixture.gzip_each(records)

      decoded =
        [gzipped] |> Archiviste.Gzip.decode_stream() |> Enum.to_list() |> IO.iodata_to_binary()

      assert decoded == WarcFixture.concat(records)
    end
  end
end
