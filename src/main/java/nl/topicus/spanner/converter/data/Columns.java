package nl.topicus.spanner.converter.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Columns
{
	private List<String> columns = new ArrayList<>();

	private List<Integer> columnTypes = new ArrayList<>();

	private List<String> primaryKeyCols = new ArrayList<>();

	public void addColumn(String name)
	{
		columns.add(name);
	}

	public void addColumnType(Integer type)
	{
		columnTypes.add(type);
	}

	public void addPrimaryKeyColumn(String name)
	{
		primaryKeyCols.add(name);
	}

	public String getColumnNames()
	{
		return String.join(", ", columns);
	}

	public String getPrimaryKeyColumns()
	{
		return String.join(", ", primaryKeyCols);
	}

	public String getPrimaryKeyColumnsWhereClause(String prefix)
	{
		List<String> res = new ArrayList<>(primaryKeyCols.size() * 2);
		for (String s : primaryKeyCols)
		{
			res.add(prefix + s + ">=?");
		}
		for (String s : primaryKeyCols)
		{
			res.add(prefix + s + "<=?");
		}
		return String.join(" AND ", res);
	}

	public String getPrimaryKeyColumns(String prefix)
	{
		List<String> res = new ArrayList<>(primaryKeyCols.size());
		for (String pk : primaryKeyCols)
			res.add(prefix + pk);
		return String.join(", ", res);
	}

	public String getColumnParameters()
	{
		String[] params = new String[columns.size()];
		Arrays.fill(params, "?");
		return String.join(", ", params);
	}

	public int getColumnIndex(String columnName)
	{
		return columns.indexOf(columnName);
	}

	public Integer getColumnType(String columnName)
	{
		int index = getColumnIndex(columnName);
		return index == -1 ? null : columnTypes.get(index);
	}

	public List<Integer> getColumnTypes()
	{
		return columnTypes;
	}

	public List<String> getPrimaryKeyCols()
	{
		return primaryKeyCols;
	}

	public List<String> getColumns()
	{
		return columns;
	}
}