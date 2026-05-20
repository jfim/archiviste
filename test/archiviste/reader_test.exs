defmodule Archiviste.ReaderTest do
  use ExUnit.Case, async: true

  alias Archiviste.Reader

  test "reads exact bytes across enumerable chunk boundaries" do
    {:ok, r} = Reader.start_link(["he", "ll", "o world"])
    assert Reader.read(r, 5) == {:ok, "hello"}
    assert Reader.read(r, 6) == {:ok, " world"}
    assert Reader.read(r, 1) == :eof
    Reader.close(r)
  end

  test "read_until/2 reads up to and including a delimiter" do
    {:ok, r} = Reader.start_link(["alpha\r\nbeta\r\n", "gamma"])
    assert Reader.read_until(r, "\r\n") == {:ok, "alpha\r\n"}
    assert Reader.read_until(r, "\r\n") == {:ok, "beta\r\n"}
    Reader.close(r)
  end

  test "skip/2 advances past N bytes without buffering" do
    {:ok, r} = Reader.start_link([String.duplicate("x", 1_000_000), "tail"])
    assert Reader.skip(r, 1_000_000) == :ok
    assert Reader.read(r, 4) == {:ok, "tail"}
    Reader.close(r)
  end

  test "offset/1 reports total consumed bytes" do
    {:ok, r} = Reader.start_link(["abcdef"])
    Reader.read(r, 3)
    assert Reader.offset(r) == 3
    Reader.skip(r, 2)
    assert Reader.offset(r) == 5
    Reader.close(r)
  end

  test "read past EOF returns :eof and stays at EOF" do
    {:ok, r} = Reader.start_link(["ab"])
    assert Reader.read(r, 4) == :eof
    assert Reader.read(r, 1) == :eof
    Reader.close(r)
  end

  test "peek/2 returns bytes without consuming them" do
    {:ok, r} = Reader.start_link(["abcdef"])
    assert Reader.peek(r, 3) == {:ok, "abc"}
    assert Reader.read(r, 3) == {:ok, "abc"}
    Reader.close(r)
  end
end
