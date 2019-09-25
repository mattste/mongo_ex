defmodule MongoExTest do
  use ExUnit.Case
  doctest MongoEx

  test "greets the world" do
    assert MongoEx.hello() == :world
  end
end
