defmodule ArchivisteTest do
  use ExUnit.Case
  doctest Archiviste

  test "greets the world" do
    assert Archiviste.hello() == :world
  end
end
