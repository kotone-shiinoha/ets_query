defmodule EtsQuery do
  @moduledoc """
    EtsQuery gives you convinient function to work with ets tables

  """
  alias :ets, as: Ets
  import Ets

  @doc """
    looks up every row in the given ets table.
    Type indicates whether the traversal should start from the start or the last row of the table.
    It would matter if the table is :ordered_set
  """
  def traversal(tab, func, type) do
    traversal(tab, first_key(tab, type), func, type)
  end

    @doc """
    looks up every row in the given ets table.
    Type indicates whether the traversal should start from the start or the last row of the table.
    It would matter if the table is :ordered_set
  """
  @spec traversal(
    atom | :ets.tid(),
    any | :'$end_of_table',
    fun,
    :first | :last
  ) :: :'$end_of_table'
  def traversal(_tab, :'$end_of_table', _func, _type), do: :'$end_of_table'
  def traversal(tab, key, func, type) do
    [outcome] = lookup(tab, key)
    func.(outcome)

    traversal(tab,
      next_key(tab, type, key),
      func,
      type
    )
  end

  defp next_key(tab, type, key) do
    case type do
      :first -> :ets.next(tab, key)
      :last -> :ets.prev(tab, key)
    end
  end

  defp first_key(tab, type) do
    case type do
      :first -> :ets.first(tab)
      :last -> :ets.last(tab)
    end
  end

  @doc """
    new_row is a function for appending an element on a ets table where the row is expected to have a list
    args: 1 => ets ref or atom, 2 => key for ets, value => an element you want to append
  """
  @spec new_row(atom | :ets.tid(), any, any) :: true
  def new_row(tab, key, value) do
    append(tab, key, value)
  end
  @spec new_row(atom | :ets.tid(), {any, any}) :: true
  def new_row(tab, tuple) do
    append(tab, tuple)
  end

  defp value2list(value) when not is_list(value), do: [value]
  defp value2list(value), do: value


  @doc """
    alias for new_row
  """
  @spec append(atom | :ets.tid(), any, any) :: true
  def append(tab, key, value) do
    append(tab, {key, value})
  end
  @spec append(atom | :ets.tid(), {any, any}) :: true
  def append(tab, {key, value}) do
    new_value =
      case lookup(tab, key) do
        [] -> value2list(value)
        [{_, existing_value}] ->
          [value | existing_value]
      end
    insert(tab, {key, new_value})
  end

  @doc """
    The row is expected to have a list as a value.
    this function will removes an element from that list
  """
  @spec remove_element(atom | :ets.tid(), any, (any -> boolean)) :: true
  def remove_element(tab, key, match_func) do
    [{_, value}] = lookup(tab, key)

    filtered = filter_element(value, match_func)

    len = length(value)
    case length(filtered) do
      ^len -> :unremoved
      _ -> insert(tab, {key, filtered})
    end
  end

  defp filter_element(to_check, func, filtered \\ [])
  defp filter_element([h | t], func, filtered) do
    filtered =
      case func.(h) do
        true -> [h | filtered]
        _ -> filtered
      end
    filter_element(t, func, filtered)
  end
  defp filter_element(_to_check, _func, filtered) do
    filtered
  end

  @spec remove(atom | :ets.tid(), any, (any -> boolean)) :: true
  def remove(tab, key, match_func) do
    remove_element(tab, key, match_func)
  end

  @spec fetch(any, :map | :list) :: any
  def fetch(tab, merge_type \\ :map) do
    {:ok, pid} = Agent.start_link(fn  -> %{} end)
    try do
      traversal(tab, fn {key, list} ->
        Agent.update(pid, &
          case merge_type do
            :map ->
                Map.merge(&1, %{ key => list })
            :list ->
                list ++ &1
          end
        )
      end, :first)
      data = Agent.get(pid, & &1)
      Agent.stop(pid)
      data
    rescue _ ->
      Agent.stop(pid)
    end
  end

end
