defmodule MongoExTest do
  use ExUnit.Case

  import Ecto.Query

  defmodule Schema do
    use Ecto.Schema

    schema "schema" do
      field(:x, :integer)
      field(:y, :integer)
      field(:z, :integer)

      has_many(:comments, MongoExTest.Schema2,
        references: :x,
        foreign_key: :z
      )

      has_one(:permalink, MongoExTest.Schema3,
        references: :y,
        foreign_key: :id
      )
    end
  end

  defmodule Schema2 do
    use Ecto.Schema

    schema "schema2" do
      belongs_to(:post, MongoExTest.Schema,
        references: :x,
        foreign_key: :z
      )
    end
  end

  defmodule Schema3 do
    use Ecto.Schema

    schema "schema3" do
      field(:binary, :binary)
    end
  end

  defp plan(query, operation \\ :all) do
    {query, _params} = Ecto.Adapter.Queryable.plan_query(operation, MongoEx, query)
    query
  end

  defp all(query, params \\ []), do: MongoEx.all(query, params)

  test "from" do
    query = Schema |> select([r], r.x) |> plan()
    assert all(query) == %{schema: "schema", query: %{}, projection: %{"x" => 1}}
  end

  test "from with hints" do
    query =
      Schema
      |> from(hints: ["foo", "bar"])
      |> select([r], r.x)
      |> plan()

    assert all(query) ==
             %{
               schema: "schema",
               query: %{},
               projection: %{"x" => 1},
               hint: %{"foo" => 1, "bar" => 1}
             }
  end

  test "select" do
    query = Schema |> select([r], {r.x, r.y}) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{},
             projection: %{"x" => 1, "y" => 1}
           }

    query = Schema |> select([r], [r.x, r.y]) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{},
             projection: %{"x" => 1, "y" => 1}
           }

    query = Schema |> select([r], struct(r, [:x, :y])) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{},
             projection: %{"x" => 1, "y" => 1}
           }

    query = Schema |> select([r], %{fieldX: r.x, fieldY: r.y}) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{},
             projection: %{"x" => 1, "y" => 1}
           }

    query = Schema |> select([r], r.x) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{},
             projection: %{"x" => 1}
           }

    query = Schema |> select([r], r) |> plan()

    assert all(query) ==
             %{
               schema: "schema",
               query: %{},
               projection: %{"id" => 1, "x" => 1, "y" => 1, "z" => 1}
             }
  end

  test "where" do
    query =
      Schema
      |> where([r], 42 == r.x or r.y < 30 or r.y > 20)
      |> select([r], r.x)
      |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{
               "$or" => [
                 %{
                   "$or" => [%{"x" => %{"$eq" => 42}}, %{"y" => %{"$lt" => 30}}]
                 },
                 %{
                   "y" => %{"$gt" => 20}
                 }
               ]
             },
             projection: %{"x" => 1}
           }

    query =
      Schema
      |> where([r], 42 == r.x)
      |> where([r], r.y != 43)
      |> select([r], r.x)
      |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{
               "$and" => [
                 %{"x" => %{"$eq" => 42}},
                 %{"y" => %{"$ne" => 43}}
               ]
             },
             projection: %{"x" => 1}
           }

    query = Schema |> where([r], {r.x, 2} > {1, r.y}) |> select([r], r.x) |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{"x" => %{"$gt" => 1}, "y" => %{"$lte" => 2}},
             projection: %{"x" => 1}
           }

    query =
      Schema
      |> where([r], r.x >= ^30)
      |> select([r], r.x)
      |> plan()

    assert all(query, [30]) == %{
             schema: "schema",
             query: %{"x" => %{"$gte" => 30}},
             projection: %{"x" => 1}
           }
  end

  test "or_where" do
    query =
      Schema
      |> where([r], r.x < 30)
      |> or_where([r], r.y > 43)
      |> select([r], r.x)
      |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{"$or" => [%{"x" => %{"$lt" => 30}}, %{"y" => %{"$gt" => 43}}]},
             projection: %{"x" => 1}
           }

    query =
      Schema
      |> or_where([r], r.x == 42)
      |> or_where([r], r.y != 43)
      |> where([r], r.z == 44)
      |> select([r], r.x)
      |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{
               "$and" => [
                 %{"$or" => [%{"x" => %{"$eq" => 42}}, %{"y" => %{"$ne" => 43}}]},
                 %{"z" => %{"$eq" => 44}}
               ]
             },
             projection: %{
               "x" => 1
             }
           }

    query =
      Schema
      |> where([r], r.x < 42)
      |> where([r], r.y > 30)
      |> or_where([r], r.x == 42)
      |> or_where([r], r.y != 43)
      |> where([r], r.z == 44)
      |> or_where([r], r.y > 50)
      |> select([r], r.x)
      |> plan()

    assert all(query) == %{
             schema: "schema",
             query: %{
               "$or" => [
                 %{
                   "$and" => [
                     %{
                       "$or" => [
                         %{
                           "$and" => [
                             %{"x" => %{"$lt" => 42}},
                             %{"y" => %{"$gt" => 30}}
                           ]
                         },
                         %{"x" => %{"$eq" => 42}},
                         %{"y" => %{"$ne" => 43}}
                       ]
                     },
                     %{
                       "z" => %{"$eq" => 44}
                     }
                   ]
                 },
                 %{"y" => %{"$gt" => 50}}
               ]
             },
             projection: %{
               "x" => 1
             }
           }
  end

  defp log_query(query) do
    IO.inspect(Map.from_struct(query))
  end
end
