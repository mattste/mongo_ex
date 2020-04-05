defmodule MongoEx do
  defguard is_field(literal) when is_atom(literal)
  defguard is_value(literal) when not is_atom(literal)

  def dumpers(_primitive, type), do: [type]

  def all(query, params) do
    normalized = %{}
    sources = []
    normalized |> from(query) |> filter(query, sources, params) |> projection(query)
  end

  defp from(normalized, query) do
    %{from: %{source: {schema_name, _schema_module}}} = query
    normalized |> Map.put(:schema, schema_name) |> normalize_hints(query)
  end

  defp normalize_hints(normalized, %{from: %{hints: hints}} = _query) when length(hints) > 0 do
    Map.put(normalized, :hint, list_to_projection(hints))
  end

  defp normalize_hints(normalized, _query), do: normalized

  defp filter(normalized, query, sources, params) do
    Map.put(normalized, :query, wheres(query, sources, params))
  end

  @where_ops %{
    and: "$and",
    or: "$or"
  }

  defp wheres(%{wheres: wheres} = query, sources, params) when length(wheres) > 1 do
    tuples = Enum.map(wheres, &expr(&1, sources, params, query)) |> Enum.zip(wheres)
    acc_wheres(tuples)
  end

  defp wheres(%{wheres: [where_expr]} = query, sources, params),
    do: expr(where_expr, sources, params, query)

  defp wheres(%{wheres: []}, _sources, _params), do: %{}

  defp acc_wheres([{expr, %{op: curr_op}} | tail]),
    do: acc_wheres(tail, curr_op, [expr])

  defp acc_wheres([{expr, %{op: curr_op}} | tail], prev_op, acc_exprs) when prev_op == curr_op do
    acc_wheres(tail, curr_op, acc_exprs ++ [expr])
  end

  defp acc_wheres([{expr, %{op: curr_op}} | tail], prev_op, acc_exprs)
       when prev_op != curr_op and length(acc_exprs) > 1 do
    acc_wheres(
      tail,
      curr_op,
      [
        %{@where_ops[prev_op] => acc_exprs},
        expr
      ]
    )
  end

  defp acc_wheres([{expr, %{op: curr_op}} | tail], prev_op, [prev_expr])
       when prev_op != curr_op do
    acc_wheres(
      tail,
      curr_op,
      [prev_expr] ++ [expr]
    )
  end

  defp acc_wheres([], prev_op, acc_expr) do
    %{@where_ops[prev_op] => acc_expr}
  end

  defp projection(normalized, query) do
    %{select: %{fields: fields}} = query
    Map.put(normalized, :projection, select_fields(fields, query))
  end

  defp select_fields([], _query) do
    %{}
  end

  defp select_fields(fields, _query) do
    Enum.map(fields, fn
      {{:., _, [{:&, [], [_idx]}, field_name]}, [], []} ->
        {normalize_field(field_name), 1}
    end)
    |> Map.new()
  end

  defp list_to_projection(list) do
    Enum.reduce(list, %{}, fn item, acc -> Map.put(acc, item, 1) end)
  end

  defp expr(%Ecto.Query.BooleanExpr{expr: expr} = _where, sources, params, query) do
    expr(expr, sources, params, query)
  end

  defp expr({:or, _, args}, sources, params, query) do
    [left, right] = args
    left = expr(left, sources, params, query)
    right = expr(right, sources, params, query)
    %{"$or" => [left, right]}
  end

  defp expr({:and, _, args}, sources, params, query) do
    [left, right] = args
    left = expr(left, sources, params, query)
    right = expr(right, sources, params, query)
    %{"$and" => [left, right]}
  end

  binary_ops = [==: "$eq", !=: "$ne", <=: "$lte", >=: "$gte", <: "$lt", >: "$gt"]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp expr({unquote(op), _, args}, sources, params, query) when unquote(op) in @binary_ops do
      normalize_comparison(unquote(str), args, sources, params, query)
    end
  end)

  defp expr({:^, _, [param_idx]}, _sources, params, _query) do
    Enum.at(params, param_idx)
  end

  defp expr({{:., _, [{:&, _, [_idx]}, field]}, _, []}, _sources, _params, _query)
       when is_atom(field) do
    field
  end

  defp expr({:{}, _, exprs}, sources, params, query) do
    Enum.map(exprs, &expr(&1, sources, params, query))
  end

  defp expr(exprs, sources, params, query) when is_list(exprs) do
    Enum.map(exprs, &expr(&1, sources, params, query))
  end

  defp expr(literal, _sources, _params, _query) when is_binary(literal), do: literal
  defp expr(literal, _sources, _params, _query) when is_integer(literal), do: literal
  defp expr(literal, _sources, _params, _query) when is_float(literal), do: literal

  defp normalize_comparison(op, [left, right] = _args, sources, params, query) do
    left = expr(left, sources, params, query)
    right = expr(right, sources, params, query)
    pairs = get_field_value_pairs(op, left, right)

    Enum.reduce(pairs, %{}, fn pair, acc ->
      [field: field, op: op, value: value] = pair
      Map.put(acc, normalize_field(field), %{op => value})
    end)
  end

  defp get_field_value_pairs(op, left_expr, right_expr)
       when is_list(left_expr) and is_list(right_expr) do
    pairs = Enum.zip(left_expr, right_expr)
    Enum.flat_map(pairs, fn {left, right} -> get_field_value_pairs(op, left, right) end)
  end

  defp get_field_value_pairs(op, left_literal, right_literal)
       when is_field(left_literal) and is_value(right_literal) do
    [[field: left_literal, op: op, value: right_literal]]
  end

  @tuple_inverse_binary_ops %{
    "$eq" => "$eq",
    "$ne" => "$ne",
    "$lte" => "$gt",
    "$gte" => "$lt",
    "$lt" => "$gte",
    "$gt" => "$lte"
  }

  defp get_field_value_pairs(op, left_literal, right_literal)
       when is_value(left_literal) and is_field(right_literal) do
    inverse_op = Map.get(@tuple_inverse_binary_ops, op)
    [[field: right_literal, op: inverse_op, value: left_literal]]
  end

  defp normalize_field(field) when is_atom(field), do: Atom.to_string(field)
end
