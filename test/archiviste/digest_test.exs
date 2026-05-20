defmodule Archiviste.DigestTest do
  use ExUnit.Case, async: true

  alias Archiviste.Digest

  test "parses and verifies sha1 with base32 encoding" do
    payload = "hello world"
    digest_bytes = :crypto.hash(:sha, payload)
    b32 = Base.encode32(digest_bytes, padding: false)
    header = "sha1:#{b32}"

    assert Digest.verify(header, payload) == :ok
  end

  test "parses and verifies sha256 with hex encoding" do
    payload = "hello world"
    digest_bytes = :crypto.hash(:sha256, payload)
    hex = Base.encode16(digest_bytes, case: :lower)
    header = "sha256:#{hex}"

    assert Digest.verify(header, payload) == :ok
  end

  test "returns {:error, :mismatch, expected, actual} on bad digest" do
    payload = "hello world"
    bad_header = "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

    assert {:error, :mismatch, "sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", _} =
             Digest.verify(bad_header, payload)
  end

  test "returns {:error, :unknown_algorithm, _} on unsupported algo" do
    assert {:error, :unknown_algorithm, "md5"} = Digest.verify("md5:abcd", "x")
  end

  test "stream API computes a digest over chunked input" do
    payload = String.duplicate("abc", 1000)
    digest_bytes = :crypto.hash(:sha, payload)
    expected_b32 = Base.encode32(digest_bytes, padding: false)

    state = Digest.init(:sha)
    state = Enum.reduce(["abc", String.duplicate("abc", 999)], state, &Digest.update(&2, &1))
    assert Digest.finalize_base32(state) == expected_b32
  end
end
