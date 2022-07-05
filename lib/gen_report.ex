defmodule GenReport do
  alias GenReport.Parser

  def build() do
    {:error, "Insira o nome de um arquivo"}
  end

  def build(filename) do
    filename
    |> Parser.parse_file()
    |> Enum.reduce(report_acc(), fn line, report ->
      update_all_hours(report, line)
      |> update_hours_per_month(line)
      |> update_hours_per_year(line)
    end)
  end

  def build_from_many(filenames) do
    filenames
    |> Task.async_stream(&build/1)
    |> Enum.reduce(report_acc(), fn {:ok, result}, report -> sum_reports(report, result) end)
  end

  defp sum_reports(
         report1,
         report2
       ),
       do: deep_merge(report1, report2)

  defp deep_merge(left, right) do
    Map.merge(left, right, &deep_resolve/3)
  end

  defp deep_resolve(_key, %{} = left, %{} = right) do
    deep_merge(left, right)
  end

  defp deep_resolve(_key, left, right) do
    right + left
  end

  defp report_acc do
    %{"all_hours" => %{}, "hours_per_month" => %{}, "hours_per_year" => %{}}
  end

  defp update_all_hours(%{"all_hours" => all_hours} = report, [name, hours, _day, _month, _year]) do
    all_hours =
      case Map.get(all_hours, name) do
        nil ->
          Map.put(all_hours, name, hours)

        value ->
          Map.put(all_hours, name, hours + value)
      end

    report
    |> Map.put("all_hours", all_hours)
  end

  defp update_hours_per_month(
         %{"hours_per_month" => hours_per_month} = report,
         [name, hours, _day, month, _year]
       ) do
    by_user = Map.get(hours_per_month, name)

    by_user_in_month =
      case by_user do
        nil ->
          nil

        _ ->
          Map.get(by_user, month)
      end

    new_hours_per_month =
      case {by_user, by_user_in_month} do
        {nil, nil} ->
          hours_per_month
          |> Map.put(name, Map.put(%{}, month, hours))

        {_by_user, nil} ->
          hours_per_month
          |> Map.put(name, Map.put(by_user, month, hours))

        {by_user, by_user_in_month} ->
          hours_per_month
          |> Map.put(name, Map.put(by_user, month, by_user_in_month + hours))
      end

    report
    |> Map.put("hours_per_month", new_hours_per_month)
  end

  defp update_hours_per_year(
         %{"hours_per_year" => hours_per_year} = report,
         [name, hours, _day, _month, year]
       ) do
    by_user = Map.get(hours_per_year, name)

    by_user_in_year =
      case by_user do
        nil ->
          nil

        _ ->
          Map.get(by_user, year)
      end

    new_hours_per_year =
      case {by_user, by_user_in_year} do
        {nil, nil} ->
          hours_per_year
          |> Map.put(name, Map.put(%{}, year, hours))

        {_by_user, nil} ->
          hours_per_year
          |> Map.put(name, Map.put(by_user, year, hours))

        {by_user, by_user_in_year} ->
          hours_per_year
          |> Map.put(name, Map.put(by_user, year, by_user_in_year + hours))
      end

    report
    |> Map.put("hours_per_year", new_hours_per_year)
  end
end
