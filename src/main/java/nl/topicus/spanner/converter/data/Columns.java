package nl.topicus.spanner.converter.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class Columns
{
	List<String> columnNames = new ArrayList<>();

	List<Integer> columnTypes = new ArrayList<>();

	List<String> primaryKeyCols = new ArrayList<>();

	String getColumnNames()
	{
		return String.join(", ", columnNames);
	}

	String getPrimaryKeyColumns()
	{
		return String.join(", ", primaryKeyCols);
	}

	String getPrimaryKeyColumnsWhereClause(String prefix)
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

	String getPrimaryKeyColumns(String prefix)
	{
		List<String> res = new ArrayList<>(primaryKeyCols.size());
		for (String pk : primaryKeyCols)
			res.add(prefix + pk);
		return String.join(", ", res);
	}

	String getColumnParameters()
	{
		String[] params = new String[columnNames.size()];
		Arrays.fill(params, "?");
		return String.join(", ", params);
	}

	int getColumnIndex(String columnName)
	{
		return columnNames.indexOf(columnName);
	}

	Integer getColumnType(String columnName)
	{
		int index = getColumnIndex(columnName);
		return index == -1 ? null : columnTypes.get(index);
	}
}