package nl.topicus.spanner.converter.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractList;
import java.util.List;

import nl.topicus.jdbc.shaded.com.google.cloud.ByteArray;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration.DatabaseType;
import nl.topicus.spanner.converter.data.Columns;

public class ConverterUtils
{
	private final ConverterConfiguration config;

	public ConverterUtils(ConverterConfiguration config)
	{
		this.config = config;
	}

	public int calculateActualBatchSize(int numberOfCols, Connection destination, String catalog, String schema,
			String table) throws SQLException
	{
		int actualBatchSize = config.getBatchSize();
		if (config.getDestinationDatabaseType() == DatabaseType.CloudSpanner)
		{
			// Calculate number of rows in a batch based on the row size
			// Batch size is given as MiB when the destination is CloudSpanner
			// The maximum number of mutations per commit is 20,000
			int rowSize = getRowSize(destination, catalog, schema, table);
			int indices = getNumberOfIndices(destination, catalog, schema, table);
			actualBatchSize = Math.max(Math.min(config.getBatchSize() / rowSize, 20000 / (numberOfCols + indices)),
					100);
		}
		return actualBatchSize;
	}

	public int getRowSize(Connection destination, String catalog, String schema, String table) throws SQLException
	{
		if (config.getDestinationDatabaseType() == DatabaseType.CloudSpanner)
			return getEstimatedRowSizeInCloudSpanner(destination, catalog, schema, table, null);
		return -1;
	}

	public int getNumberOfIndices(Connection destination, String catalog, String schema, String table)
			throws SQLException
	{
		int count = 0;
		try (ResultSet indices = destination.getMetaData().getIndexInfo(catalog, schema, table, false, false))
		{
			while (indices.next())
				count++;
		}
		return count;
	}

	/**
	 * @param connection
	 *            The connection to use to estimate the row size
	 * @param catalog
	 *            The catalog of the table
	 * @param schemaPattern
	 *            The schema of the table
	 * @param tableNamePattern
	 *            The name of the table
	 * @param columnNamePattern
	 *            The name of the column(s) to check
	 * @return The estimated size in bytes of one row of the specified columns
	 *         of the specified table
	 */
	public int getEstimatedRowSizeInCloudSpanner(Connection connection, String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern) throws SQLException
	{
		// There's an 8 bytes storage overhead for each column
		int totalSize = 8;
		try (ResultSet rs = connection.getMetaData().getColumns(catalog, schemaPattern, tableNamePattern,
				columnNamePattern))
		{
			while (rs.next())
			{
				long colLength = rs.getLong("COLUMN_SIZE");
				int colType = rs.getInt("DATA_TYPE");
				switch (colType)
				{
				case Types.ARRAY:
					break;
				case Types.BOOLEAN:
					totalSize += 1;
					break;
				case Types.BINARY:
					totalSize += colLength;
					break;
				case Types.DATE:
					totalSize += 4;
					break;
				case Types.DOUBLE:
					totalSize += 8;
					break;
				case Types.BIGINT:
					totalSize += 8;
					break;
				case Types.NVARCHAR:
					totalSize += colLength * 2;
					break;
				case Types.TIMESTAMP:
					totalSize += 12;
					break;
				}
			}
		}
		return totalSize;
	}

	public int getActualDataSize(int colType, Object data)
	{
		int size = 0;
		switch (colType)
		{
		case Types.ARRAY:
			break;
		case Types.BOOLEAN:
			size = 1;
			break;
		case Types.BINARY:
			if (data != null && byte[].class.equals(data.getClass()))
				size = ((byte[]) data).length;
			else if (data != null && ByteArray.class.equals(data))
				size = ((ByteArray) data).length();
			break;
		case Types.DATE:
			size = 4;
			break;
		case Types.DOUBLE:
			size = 8;
			break;
		case Types.BIGINT:
			size = 8;
			break;
		case Types.NVARCHAR:
			if (data != null && String.class.equals(data.getClass()))
				size = ((String) data).getBytes().length;
			break;
		case Types.TIMESTAMP:
			size = 12;
			break;
		}
		return size;
	}

	public String getTableSpec(String catalog, String schema, String table)
	{
		String tableSpec = "";
		if (catalog != null && !"".equals(catalog))
			tableSpec = tableSpec + catalog + ".";
		if (schema != null && !"".equals(schema) && !"public".equals(schema))
			tableSpec = tableSpec + schema + ".";
		tableSpec = tableSpec + table;

		return tableSpec;
	}

	public Columns getColumns(Connection destination, String catalog, String schema, String table, boolean forSelect)
			throws SQLException
	{
		Columns res = new Columns();
		try (ResultSet columns = destination.getMetaData().getColumns(catalog, schema, table, null))
		{
			while (columns.next())
			{
				// When doing a select and a column is named the same as the
				// table, Cloud Spanner will misinterpret the query. In those
				// cases, the column name will be prefixed by the table name
				res.addColumn(forSelect && columns.getString("COLUMN_NAME").equalsIgnoreCase(table)
						? table + "." + columns.getString("COLUMN_NAME") : columns.getString("COLUMN_NAME"));
				res.addColumnType(columns.getInt("DATA_TYPE"));
			}
		}
		try (ResultSet keys = destination.getMetaData().getPrimaryKeys(catalog, schema, table))
		{
			while (keys.next())
			{
				res.addPrimaryKeyColumn(keys.getString("COLUMN_NAME"));
			}
		}
		return res;
	}

	public int getSourceRecordCount(Connection source, String tableSpec) throws SQLException
	{
		try (ResultSet count = source.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableSpec))
		{
			if (count.next())
				return count.getInt(1);
		}
		return 0;
	}

	public long getDestinationRecordCount(Connection destination, String table) throws SQLException
	{
		String sql = "select count(*) from " + table;
		try (ResultSet rs = destination.createStatement().executeQuery(sql))
		{
			if (rs.next())
				return rs.getLong(1);
		}
		finally
		{
			destination.commit();
		}
		return 0;
	}

	public static <T> List<List<T>> partition(final List<T> list, final int size)
	{
		if (list == null)
		{
			throw new NullPointerException("List must not be null");
		}
		if (size <= 0)
		{
			throw new IllegalArgumentException("Size must be greater than 0");
		}
		return new Partition<>(list, size);
	}

	/**
	 * Provides a partition view on a {@link List}.
	 * 
	 * @since 4.0
	 */
	private static class Partition<T> extends AbstractList<List<T>>
	{
		private final List<T> list;
		private final int size;

		private Partition(final List<T> list, final int size)
		{
			this.list = list;
			this.size = size;
		}

		@Override
		public List<T> get(final int index)
		{
			final int listSize = size();
			if (listSize < 0)
			{
				throw new IllegalArgumentException("negative size: " + listSize);
			}
			if (index < 0)
			{
				throw new IndexOutOfBoundsException("Index " + index + " must not be negative");
			}
			if (index >= listSize)
			{
				throw new IndexOutOfBoundsException("Index " + index + " must be less than size " + listSize);
			}
			final int start = index * size;
			final int end = Math.min(start + size, list.size());
			return list.subList(start, end);
		}

		@Override
		public int size()
		{
			return (list.size() + size - 1) / size;
		}

		@Override
		public boolean isEmpty()
		{
			return list.isEmpty();
		}
	}

}
