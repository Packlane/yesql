defmodule Yesql do
  @moduledoc """

      defmodule Query do
        use Yesql, driver: Postgrex, conn: MyApp.ConnectionPool

        Yesql.defquery("some/where/select_users_by_country.sql")
      end

      Query.users_by_country(country_code: "gbr")
      # => {:ok, [%{name: "Louis", country_code: "gbr"}]}

  ## Supported drivers

  - `Postgrex`
  - `Ecto`, for which `conn` is an Ecto repo.
  """

  alias __MODULE__.{NoDriver, UnknownDriver, MissingParam}

  @supported_drivers [Postgrex, Ecto]

  defmacro __using__(opts) do
    quote bind_quoted: binding() do
      @yesql_private__driver opts[:driver]
      @yesql_private__conn opts[:conn]
    end
  end

  defmacro defqueries(file_path, opts \\ []) do
    drivers = @supported_drivers

    quote bind_quoted: binding() do
      name = file_path |> Path.basename(".sql") |> String.to_atom()
      driver = opts[:driver] || @yesql_private__driver || raise(NoDriver, name)
      unless driver in drivers, do: raise(UnknownDriver, driver)

      conn = opts[:conn] || @yesql_private__conn

      {:ok, blocks} =
        file_path
        |> File.read!()
        |> Yesql.parse_many()

      Enum.each(blocks, fn %{
                             name: name,
                             description: desc,
                             param_spec: param_spec,
                             tokenized_sql: sql
                           } ->
        # doc = [unquote(name), "args: " <> unquote(param_spec), unquote(desc)] |> Enum.join("\n")

        @doc """
        unquote(name)
        Documentation for fn/1
        """
        def unquote(name)(conn, args) do
          Yesql.exec(conn, unquote(driver), unquote(sql), unquote(param_spec), args)
        end

        if conn do
          # Module.put_attribute(module, :doc, """
          # unquote(name)/1:
          # args: unquote(param_spec)
          # unquote(desc)
          # """)

          # @doc [unquote(name), "args: " <> unquote(param_spec), unquote(desc)] |> Enum.join("\n")
          @doc """
          unquote(name)
          Documentation for fn/1
          """
          def unquote(name)(args) do
            Yesql.exec(unquote(conn), unquote(driver), unquote(sql), unquote(param_spec), args)
          end
        end
      end)
    end
  end

  defmacro defquery(file_path, opts \\ []) do
    drivers = @supported_drivers

    quote bind_quoted: binding() do
      name = file_path |> Path.basename(".sql") |> String.to_atom()
      driver = opts[:driver] || @yesql_private__driver || raise(NoDriver, name)
      conn = opts[:conn] || @yesql_private__conn
      {:ok, sql, param_spec} = file_path |> File.read!() |> Yesql.parse()

      unless driver in drivers, do: raise(UnknownDriver, driver)

      def unquote(name)(conn, args) do
        Yesql.exec(conn, unquote(driver), unquote(sql), unquote(param_spec), args)
      end

      if conn do
        def unquote(name)(args) do
          Yesql.exec(unquote(conn), unquote(driver), unquote(sql), unquote(param_spec), args)
        end
      end
    end
  end

  @doc false
  def parse(sql) do
    with {:ok, tokens, _} <- Yesql.Tokenizer.tokenize(sql) do
      {_, query_iodata, params_pairs} =
        tokens
        |> Enum.reduce({1, [], []}, &extract_param/2)

      sql = IO.iodata_to_binary(query_iodata)
      params = params_pairs |> Keyword.keys() |> Enum.reverse()

      {:ok, sql, params}
    end
  end

  @doc false
  def parse_many(sql) do
    parsed_blocks =
      sql
      |> split_to_blocks()
      |> Enum.map(&parse_block/1)

    result =
      parsed_blocks
      |> Enum.map(fn x ->
        sql = Map.get(x, :sql)
        {:ok, tokenized_sql, args} = parse(sql)

        x
        |> Map.put(:tokenized_sql, tokenized_sql)
        |> Map.put(:param_spec, args)
      end)

    {:ok, result}
  end

  def ensure_trailing(s, char) do
    cond do
      String.ends_with?(s, char) -> s
      true -> s |> Kernel.<>(char)
    end
  end

  def join_with_trailing(enum, char) do
    enum |> Enum.join(char) |> ensure_trailing("\n")
  end

  def parse_block(block) do
    {h, tail} =
      block
      |> String.split("\n")
      |> Enum.split_while(&String.starts_with?(&1, "--"))

    {name, description} =
      case h do
        [name] -> {name, []}
        [name | rest] -> {name, rest}
      end

    name =
      name
      |> String.split(":")
      |> List.last()
      |> String.trim()
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      # Safety check to make sure it's valid atom and thus likely a valid fn
      |> String.to_atom()

    description =
      description
      |> Enum.map(&String.trim_leading(&1, "--"))
      |> Enum.map(&String.trim/1)
      |> Enum.join("\n")

    %{name: name, description: description, sql: tail |> join_with_trailing("\n")}
  end

  def split_to_blocks(sql) do
    # Parse many expression
    Regex.split(~r/-- name:/, sql)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&("-- name: " <> &1))
    |> Enum.map(fn x -> ensure_trailing(x, "\n") end)
  end

  defp extract_param({:named_param, param}, {i, sql, params}) do
    case params[param] do
      nil ->
        {i + 1, [sql, "$#{i}"], [{param, i} | params]}

      num ->
        {i, [sql, "$#{num}"], params}
    end
  end

  defp extract_param({:fragment, fragment}, {i, sql, params}) do
    {i, [sql, fragment], params}
  end

  @doc false
  def exec(conn, driver, sql, param_spec, data) do
    param_list = Enum.map(param_spec, &fetch_param(data, &1))

    with {:ok, result} <- exec_for_driver(conn, driver, sql, param_list) do
      format_result(result)
    end
  end

  defp fetch_param(data, key) do
    case dict_fetch(data, key) do
      {:ok, value} -> value
      :error -> raise(MissingParam, key)
    end
  end

  defp dict_fetch(dict, key) when is_map(dict), do: Map.fetch(dict, key)
  defp dict_fetch(dict, key) when is_list(dict), do: Keyword.fetch(dict, key)

  if Code.ensure_compiled?(Postgrex) do
    defp exec_for_driver(conn, Postgrex, sql, param_list) do
      Postgrex.query(conn, sql, param_list)
    end
  end

  if Code.ensure_compiled?(Ecto) do
    defp exec_for_driver(repo, Ecto, sql, param_list) do
      Ecto.Adapters.SQL.query(repo, sql, param_list)
    end
  end

  defp exec_for_driver(_, driver, _, _) do
    raise UnknownDriver.exception(driver)
  end

  defp format_result(result) do
    atom_columns = Enum.map(result.columns || [], &String.to_atom/1)

    result =
      Enum.map(result.rows || [], fn row ->
        atom_columns |> Enum.zip(row) |> Enum.into(%{})
      end)

    {:ok, result}
  end
end
