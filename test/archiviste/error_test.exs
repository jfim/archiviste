defmodule Archiviste.ErrorTest do
  use ExUnit.Case, async: true

  alias Archiviste.Error

  test "MalformedRecordError carries offset and reason" do
    err = %Error.MalformedRecordError{offset: 42, reason: :bad_header}
    assert Exception.message(err) =~ "offset 42"
    assert Exception.message(err) =~ "bad_header"
  end

  test "TruncatedFileError carries offset" do
    err = %Error.TruncatedFileError{offset: 100}
    assert Exception.message(err) =~ "offset 100"
  end

  test "DigestMismatchError carries record id and which digest" do
    err = %Error.DigestMismatchError{
      record_id: "<urn:uuid:abc>",
      digest_kind: :block,
      expected: "sha1:AAAA",
      actual: "sha1:BBBB"
    }

    msg = Exception.message(err)
    assert msg =~ "<urn:uuid:abc>"
    assert msg =~ "block"
    assert msg =~ "AAAA"
    assert msg =~ "BBBB"
  end

  test "UnsupportedEncodingError suggests the dep to add" do
    err = %Error.UnsupportedEncodingError{encoding: "br"}
    msg = Exception.message(err)
    assert msg =~ "br"
    assert msg =~ ":brotli"
  end
end
