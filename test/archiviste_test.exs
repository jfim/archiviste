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

    records =
      chunks
      |> Archiviste.stream!()
      |> Stream.map(fn record ->
        %{record | payload: [Record.read_payload(record)]}
      end)
      |> Enum.to_list()

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
      |> Stream.map(fn record ->
        %{record | payload: [Record.read_payload(record)]}
      end)
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

  test "stream_file!/2 auto-detects per-record gzip from .gz extension" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "first"),
        WarcFixture.record(type: "response", payload: "second")
      ])

    path =
      Path.join(
        System.tmp_dir!(),
        "archiviste_test_#{System.unique_integer([:positive])}.warc.gz"
      )

    File.write!(path, members)

    try do
      records =
        path
        |> Archiviste.stream_file!()
        |> Stream.map(fn r -> %{r | payload: [Archiviste.Record.read_payload(r)]} end)
        |> Enum.to_list()

      assert Enum.map(records, & &1.type) == [:warcinfo, :response]
      assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["first", "second"]
    after
      File.rm(path)
    end
  end

  test "stream_file!/2 auto-detects gzip from magic bytes when extension is plain" do
    members =
      WarcFixture.gzip_each([
        WarcFixture.record(type: "warcinfo", payload: "magic")
      ])

    path =
      Path.join(System.tmp_dir!(), "archiviste_test_#{System.unique_integer([:positive])}.warc")

    File.write!(path, members)

    try do
      records =
        path
        |> Archiviste.stream_file!()
        |> Stream.map(fn r -> %{r | payload: [Archiviste.Record.read_payload(r)]} end)
        |> Enum.to_list()

      assert Enum.map(records, & &1.type) == [:warcinfo]
      assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["magic"]
    after
      File.rm(path)
    end
  end

  test "lenient mode: malformed record is skipped, subsequent records yield" do
    good1 = WarcFixture.record(type: "warcinfo", payload: "ok1")
    # A malformed record: invalid version line.
    bad = "WARC/garbage\r\n\r\n"
    good2 = WarcFixture.record(type: "response", payload: "ok2")

    bytes = good1 <> bad <> good2

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [bytes]
        |> Archiviste.stream!()
        |> Stream.map(fn r -> %{r | payload: [Archiviste.Record.read_payload(r)]} end)
        |> Enum.to_list()
      end)

    assert Enum.map(records, &Archiviste.Record.read_payload/1) == ["ok1", "ok2"]
    assert log =~ "skipped malformed record"
  end

  test "strict mode: malformed record raises mid-stream" do
    good = WarcFixture.record(type: "warcinfo", payload: "ok")
    bad = "WARC/garbage\r\n\r\n"

    assert_raise Archiviste.Error.MalformedRecordError, fn ->
      [good <> bad] |> Archiviste.stream!(strict: true) |> Enum.to_list()
    end
  end

  test "strict: truncated payload raises TruncatedFileError when consumed" do
    full = WarcFixture.record(type: "response", payload: "abcdefghij")
    truncated = binary_part(full, 0, byte_size(full) - 5)

    assert_raise Archiviste.Error.TruncatedFileError, fn ->
      [truncated]
      |> Archiviste.stream!(strict: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)
    end
  end

  test "lenient: truncated header is skipped with a log warning" do
    full = WarcFixture.record(type: "response", payload: "abc")
    truncated_header = binary_part(full, 0, 30)

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [truncated_header] |> Archiviste.stream!() |> Enum.to_list()
      end)

    assert records == []
    assert log =~ "malformed" or log =~ "truncated"
  end

  test "verify_digests: true verifies WARC-Block-Digest and passes on match" do
    payload = "matched-payload"

    digest =
      "sha1:" <> (payload |> then(&:crypto.hash(:sha, &1)) |> Base.encode32(padding: false))

    bytes =
      WarcFixture.record(
        type: "resource",
        headers: [{"WARC-Block-Digest", digest}],
        payload: payload
      )

    [read_payload] =
      [bytes]
      |> Archiviste.stream!(verify_digests: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)

    assert read_payload == payload
  end

  test "strict + verify_digests: digest mismatch raises DigestMismatchError" do
    payload = "actual-payload"

    bytes =
      WarcFixture.record(
        type: "resource",
        headers: [{"WARC-Block-Digest", "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}],
        payload: payload
      )

    assert_raise Archiviste.Error.DigestMismatchError, fn ->
      [bytes]
      |> Archiviste.stream!(strict: true, verify_digests: true)
      |> Enum.map(&Archiviste.Record.read_payload/1)
    end
  end

  test "lenient + verify_digests: digest mismatch is skipped with a log warning" do
    payload = "actual-payload"

    bytes =
      WarcFixture.record(
        type: "resource",
        headers: [{"WARC-Block-Digest", "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}],
        payload: payload
      )

    import ExUnit.CaptureLog

    {records, log} =
      with_log(fn ->
        [bytes]
        |> Archiviste.stream!(verify_digests: true)
        |> Enum.to_list()
      end)

    assert records == []
    assert log =~ "digest_mismatch" or log =~ "digest"
  end

  test "read_at!/3 reads a single record at a known offset (plain file)" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "second")
    bytes = a <> b

    path =
      Path.join(
        System.tmp_dir!(),
        "archiviste_test_at_#{System.unique_integer([:positive])}.warc"
      )

    File.write!(path, bytes)

    try do
      offset = byte_size(a)
      record = Archiviste.read_at!(path, offset)
      assert record.type == :response
      assert Archiviste.Record.read_payload(record) == "second"
    after
      File.rm(path)
    end
  end

  test "read_at!/3 reads a single record at a known offset (.warc.gz file)" do
    a = WarcFixture.record(type: "warcinfo", payload: "a")
    b = WarcFixture.record(type: "response", payload: "second")
    a_gz = WarcFixture.gzip(a)
    b_gz = WarcFixture.gzip(b)
    bytes = a_gz <> b_gz

    path =
      Path.join(
        System.tmp_dir!(),
        "archiviste_test_at_#{System.unique_integer([:positive])}.warc.gz"
      )

    File.write!(path, bytes)

    try do
      offset = byte_size(a_gz)
      record = Archiviste.read_at!(path, offset)
      assert record.type == :response
      assert Archiviste.Record.read_payload(record) == "second"
    after
      File.rm(path)
    end
  end
end
