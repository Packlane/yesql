defmodule YesqlTest do
  use ExUnit.Case
  doctest Yesql
  import TestHelper

  defmodule Query do
    use Yesql, driver: Postgrex

    Yesql.defquery("test/sql/select_older_cats.sql")
    Yesql.defquery("test/sql/insert_cat.sql")
  end

  defmodule QueryConn do
    use Yesql, driver: Postgrex, conn: YesqlTest.Postgrex
    Yesql.defquery("test/sql/select_older_cats.sql")
    Yesql.defquery("test/sql/insert_cat.sql")
  end

  defmodule QueryEcto do
    use Yesql, driver: Ecto, conn: YesqlTest.Repo
    Yesql.defquery("test/sql/select_older_cats.sql")
    Yesql.defquery("test/sql/insert_cat.sql")
  end

  defmodule QueryMany do
    use Yesql, driver: Postgrex

    Yesql.defqueries("test/sql/two_queries.sql")
  end

  defmodule QueryManyConn do
    use Yesql, driver: Postgrex, conn: YesqlTest.Postgrex
    Yesql.defqueries("test/sql/two_queries.sql")
  end

  defmodule QueryManyEcto do
    use Yesql, driver: Ecto, conn: YesqlTest.Repo
    Yesql.defqueries("test/sql/two_queries.sql")
  end

  setup_all [:new_postgrex_connection, :create_cats_postgres_table]

  describe "parse/1" do
    import Yesql, only: [parse: 1]

    test "simple tests" do
      assert parse("SELECT * FROM person WHERE age > 18") ==
               {:ok, "SELECT * FROM person WHERE age > 18", []}

      assert parse("SELECT * FROM person WHERE age > :age") ==
               {:ok, "SELECT * FROM person WHERE age > $1", [:age]}

      assert parse("SELECT * FROM person WHERE :age > age") ==
               {:ok, "SELECT * FROM person WHERE $1 > age", [:age]}

      assert parse("SELECT 1 FROM dual") == {:ok, "SELECT 1 FROM dual", []}
      assert parse("SELECT :value FROM dual") == {:ok, "SELECT $1 FROM dual", [:value]}
      assert parse("SELECT 'test' FROM dual") == {:ok, "SELECT 'test' FROM dual", []}
      assert parse("SELECT 'test'\nFROM dual") == {:ok, "SELECT 'test'\nFROM dual", []}

      assert parse("SELECT :value, :other_value FROM dual") ==
               {:ok, "SELECT $1, $2 FROM dual", [:value, :other_value]}
    end

    test "Tokenization rules" do
      assert parse("SELECT :age-5 FROM dual") == {:ok, "SELECT $1-5 FROM dual", [:age]}
    end

    test "escapes" do
      assert parse("SELECT :value, :other_value, ':not_a_value' FROM dual") ==
               {:ok, "SELECT $1, $2, ':not_a_value' FROM dual", [:value, :other_value]}

      assert parse(~S"SELECT 'not \' :a_value' FROM dual") ==
               {:ok, ~S"SELECT 'not \' :a_value' FROM dual", []}
    end

    test "casting" do
      assert parse("SELECT :value, :other_value, 5::text FROM dual") ==
               {:ok, "SELECT $1, $2, 5::text FROM dual", [:value, :other_value]}
    end

    test "newlines are preserved" do
      assert parse("SELECT :value, :other_value, 5::text\nFROM dual") ==
               {:ok, "SELECT $1, $2, 5::text\nFROM dual", [:value, :other_value]}
    end

    test "complex 1" do
      assert parse("SELECT :a+2*:b+age::int FROM users WHERE username = :name AND :b > 0") ==
               {
                 :ok,
                 "SELECT $1+2*$2+age::int FROM users WHERE username = $3 AND $2 > 0",
                 [:a, :b, :name]
               }
    end

    test "complex 2" do
      assert parse("SELECT :value1 + :value2 + value3 + :value4 + :value1\nFROM SYSIBM.SYSDUMMY1") ==
               {
                 :ok,
                 "SELECT $1 + $2 + value3 + $3 + $1\nFROM SYSIBM.SYSDUMMY1",
                 [:value1, :value2, :value4]
               }
    end

    test "complex 3" do
      assert parse("SELECT ARRAY [:value1] FROM dual") ==
               {:ok, "SELECT ARRAY [$1] FROM dual", [:value1]}
    end
  end

  describe "parse_many/1" do
    import Yesql,
      only: [parse_block: 1, parse_many: 1, parse: 1, split_to_blocks: 1]

    test "simple tests" do
      query = """
      -- name: user-count
      -- Counts all the users.
      SELECT count(*) AS count FROM user
      """

      two_queries = """
      -- name: users-by-country
      -- Counts the users in a given country.
      -- Second line
      SELECT count(*) AS count
      FROM user
      WHERE country_code = :country_code

      -- name: user-count
      -- Counts all the users.
      SELECT count(*) AS count FROM user
      """

      assert split_to_blocks(two_queries) |> Enum.count() == 2

      assert split_to_blocks(two_queries) |> List.last() == """
             -- name: user-count
             -- Counts all the users.
             SELECT count(*) AS count FROM user
             """

      assert parse_block(query) == %{
               name: :user_count,
               description: "Counts all the users.",
               sql: "SELECT count(*) AS count FROM user\n"
             }

      two_query_blocks = [
        %{
          description: "Counts the users in a given country.\nSecond line",
          name: :users_by_country,
          sql: "SELECT count(*) AS count\nFROM user\nWHERE country_code = :country_code\n"
        },
        %{
          description: "Counts all the users.",
          name: :user_count,
          sql: "SELECT count(*) AS count FROM user\n"
        }
      ]

      two_query_parsed = [
        %{
          description: "Counts the users in a given country.\nSecond line",
          name: :users_by_country,
          sql: "SELECT count(*) AS count\nFROM user\nWHERE country_code = :country_code\n",
          param_spec: [:country_code],
          tokenized_sql: "SELECT count(*) AS count\nFROM user\nWHERE country_code = $1\n"
        },
        %{
          description: "Counts all the users.",
          name: :user_count,
          sql: "SELECT count(*) AS count FROM user\n",
          param_spec: [],
          tokenized_sql: "SELECT count(*) AS count FROM user\n"
        }
      ]

      assert split_to_blocks(two_queries) |> Enum.map(&parse_block/1) == two_query_blocks
      assert parse_many(two_queries) == {:ok, two_query_parsed}

      result =
        "test/sql/two_queries.sql"
        |> File.read!()
        |> Yesql.parse_many()

      assert result ==
               {:ok,
                [
                  %{
                    description: "Important to find old cats",
                    name: :select_older_cats,
                    sql: "SELECT * FROM cats\nWHERE age > :age\nORDER BY age ASC\n",
                    param_spec: [:age],
                    tokenized_sql: "SELECT * FROM cats\nWHERE age > $1\nORDER BY age ASC\n"
                  },
                  %{
                    description: "",
                    name: :insert_cat,
                    sql: "INSERT INTO cats (age)\nVALUES (:age)\n",
                    param_spec: [:age],
                    tokenized_sql: "INSERT INTO cats (age)\nVALUES ($1)\n"
                  }
                ]}
    end
  end

  describe "exec/4" do
    setup [:truncate_postgres_cats]

    test "unknown driver" do
      assert_raise Yesql.UnknownDriver, "Unknown database driver Elixir.Boopatron\n", fn ->
        Yesql.exec(self(), Boopatron, "", [], %{})
      end
    end

    test "Postgrex insert", ctx do
      sql = "INSERT INTO cats (age) VALUES ($1)"
      assert {:ok, []} = Yesql.exec(ctx.postgrex, Postgrex, sql, [:age], %{age: 5})
    end

    test "Postgrex insert returning columns", ctx do
      sql = "INSERT INTO cats (age) VALUES ($1), (10) RETURNING age"

      assert Yesql.exec(ctx.postgrex, Postgrex, sql, [:age], %{age: 5}) ==
               {:ok, [%{age: 5}, %{age: 10}]}
    end

    test "Postgrex select", ctx do
      insert_sql = "INSERT INTO cats (age) VALUES ($1), (10)"
      assert {:ok, []} = Yesql.exec(ctx.postgrex, Postgrex, insert_sql, [:age], %{age: 5})
      sql = "SELECT * FROM cats"
      assert {:ok, results} = Yesql.exec(ctx.postgrex, Postgrex, sql, [], %{})
      assert results == [%{age: 5, name: nil}, %{age: 10, name: nil}]
    end

    test "Postgrex invalid insert", ctx do
      insert_sql = "INSERT INTO cats (size) VALUES ($1), (10)"
      assert {:error, error} = Yesql.exec(ctx.postgrex, Postgrex, insert_sql, [:age], %{age: 1})
      assert error.postgres.message == "column \"size\" of relation \"cats\" does not exist"
    end
  end

  describe "defqueries/2" do
    setup [:truncate_postgres_cats]

    test "query function is created" do
      refute function_exported?(QueryMany, :select_older_cats, 1)
      assert function_exported?(QueryMany, :select_older_cats, 2)

      # The /1 arity function is called because conn isn't needed.
      assert function_exported?(QueryManyConn, :select_older_cats, 1)
      assert function_exported?(QueryManyConn, :select_older_cats, 2)
    end

    test "throws if map argument missing" do
      assert_raise Yesql.MissingParam, "Required parameter `:age` not given\n", fn ->
        QueryManyConn.select_older_cats(%{})
      end
    end

    test "throws if keyword argument missing" do
      assert_raise Yesql.MissingParam, "Required parameter `:age` not given\n", fn ->
        QueryManyConn.select_older_cats(nil, [])
      end
    end

    test "query exec with explicit conn", %{postgrex: conn} do
      assert QueryMany.select_older_cats(conn, age: 5) == {:ok, []}
      assert QueryMany.insert_cat(conn, age: 50) == {:ok, []}
      assert QueryMany.select_older_cats(conn, age: 5) == {:ok, [%{age: 50, name: nil}]}
      assert QueryMany.insert_cat(conn, age: 10) == {:ok, []}

      assert QueryMany.select_older_cats(conn, age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}

      assert QueryMany.insert_cat(conn, age: 1) == {:ok, []}

      assert QueryMany.select_older_cats(conn, age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}
    end

    test "query exec with implicit conn" do
      assert QueryManyConn.select_older_cats(age: 5) == {:ok, []}
      assert QueryManyConn.insert_cat(age: 50) == {:ok, []}
      assert QueryManyConn.select_older_cats(age: 5) == {:ok, [%{age: 50, name: nil}]}
      assert QueryManyConn.insert_cat(age: 10) == {:ok, []}

      assert QueryManyConn.select_older_cats(age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}

      assert QueryManyConn.insert_cat(age: 1) == {:ok, []}

      assert QueryManyConn.select_older_cats(age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}
    end

    test "query exec with Ecto driver" do
      assert QueryManyEcto.select_older_cats(age: 5) == {:ok, []}
      assert QueryManyEcto.insert_cat(age: 50) == {:ok, []}
      assert QueryManyEcto.select_older_cats(age: 5) == {:ok, [%{age: 50, name: nil}]}
    end
  end

  describe "defquery/2" do
    setup [:truncate_postgres_cats]

    test "query function is created" do
      refute function_exported?(Query, :select_older_cats, 1)
      assert function_exported?(Query, :select_older_cats, 2)

      # The /1 arity function is called because conn isn't needed.
      assert function_exported?(QueryConn, :select_older_cats, 1)
      assert function_exported?(QueryConn, :select_older_cats, 2)
    end

    test "throws if map argument missing" do
      assert_raise Yesql.MissingParam, "Required parameter `:age` not given\n", fn ->
        QueryConn.select_older_cats(%{})
      end
    end

    test "throws if keyword argument missing" do
      assert_raise Yesql.MissingParam, "Required parameter `:age` not given\n", fn ->
        QueryConn.select_older_cats(nil, [])
      end
    end

    test "query exec with explicit conn", %{postgrex: conn} do
      assert Query.select_older_cats(conn, age: 5) == {:ok, []}
      assert Query.insert_cat(conn, age: 50) == {:ok, []}
      assert Query.select_older_cats(conn, age: 5) == {:ok, [%{age: 50, name: nil}]}
      assert Query.insert_cat(conn, age: 10) == {:ok, []}

      assert Query.select_older_cats(conn, age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}

      assert Query.insert_cat(conn, age: 1) == {:ok, []}

      assert Query.select_older_cats(conn, age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}
    end

    test "query exec with implicit conn" do
      assert QueryConn.select_older_cats(age: 5) == {:ok, []}
      assert QueryConn.insert_cat(age: 50) == {:ok, []}
      assert QueryConn.select_older_cats(age: 5) == {:ok, [%{age: 50, name: nil}]}
      assert QueryConn.insert_cat(age: 10) == {:ok, []}

      assert QueryConn.select_older_cats(age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}

      assert QueryConn.insert_cat(age: 1) == {:ok, []}

      assert QueryConn.select_older_cats(age: 5) ==
               {:ok, [%{age: 10, name: nil}, %{age: 50, name: nil}]}
    end

    test "query exec with Ecto driver" do
      assert QueryEcto.select_older_cats(age: 5) == {:ok, []}
      assert QueryEcto.insert_cat(age: 50) == {:ok, []}
      assert QueryEcto.select_older_cats(age: 5) == {:ok, [%{age: 50, name: nil}]}
    end
  end
end
