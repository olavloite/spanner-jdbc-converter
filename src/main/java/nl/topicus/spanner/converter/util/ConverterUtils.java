package nl.topicus.spanner.converter.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractList;
import java.util.List;

public class ConverterUtils
{

	/**
	 * 
	 * @return The estimated size in bytes of one row of the specified columns
	 *         of the specified table
	 */
	public static int getEstimatedRowSizeInCloudSpanner(Connection connection, String catalog, String schemaPattern,
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
