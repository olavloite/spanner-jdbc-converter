package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

public class DeleteWorker implements Runnable
{
	private static final Logger log = Logger.getLogger(DeleteWorker.class.getName());

	private final String name;

	private String selectFormat = "SELECT $COLUMNS FROM $TABLE WHERE $WHERE_CLAUSE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE";

	private String table;

	private Columns columns;

	private List<Object> beginKey;

	private List<Object> endKey;

	private long numberOfRecordsToDelete;

	private int batchSize;

	private String urlDestination;

	private boolean useJdbcBatching;

	private SQLException exception;

	private long recordCount;

	DeleteWorker(String name, String table, Columns columns, List<Object> beginKey, List<Object> endKey,
			long numberOfRecordsToDelete, int batchSize, String urlDestination, boolean useJdbcBatching)
	{
		this.name = name;
		this.table = table;
		this.columns = columns;
		this.beginKey = beginKey;
		this.endKey = endKey;
		this.numberOfRecordsToDelete = numberOfRecordsToDelete;
		this.batchSize = batchSize;
		this.urlDestination = urlDestination;
		this.useJdbcBatching = useJdbcBatching;
	}

	@Override
	public void run()
	{
		try (Connection destination = DriverManager.getConnection(urlDestination);
				Connection selectConnection = DriverManager.getConnection(urlDestination))
		{
			long startTime = System.currentTimeMillis();
			log.fine(name + ": " + table + ": Starting deleting " + numberOfRecordsToDelete + " records");

			selectConnection.setReadOnly(true);
			destination.setAutoCommit(false);

			boolean first = true;
			String sql = "DELETE FROM " + table + " WHERE ";
			for (String col : columns.primaryKeyCols)
			{
				if (!first)
					sql = sql + " AND ";
				sql = sql + col + "=?";
				first = false;
			}
			PreparedStatement statement = destination.prepareStatement(sql);

			int limit = batchSize;
			String select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(table + "."));
			select = select.replace("$TABLE", table);
			select = select.replace("$WHERE_CLAUSE", columns.getPrimaryKeyColumnsWhereClause(table + "."));
			select = select.replace("$PRIMARY_KEY", columns.getPrimaryKeyColumns());
			select = select.replace("$BATCH_SIZE", String.valueOf(limit));
			PreparedStatement selectStatement = selectConnection.prepareStatement(select);
			int paramIndex = 1;
			for (Object beginKeyValue : beginKey)
			{
				selectStatement.setObject(paramIndex, beginKeyValue);
				paramIndex++;
			}
			for (Object endKeyValue : endKey)
			{
				selectStatement.setObject(paramIndex, endKeyValue);
				paramIndex++;
			}

			int recordCount = 0;
			while (true)
			{
				boolean recordsFound = false;
				try (ResultSet rs = selectStatement.executeQuery())
				{
					while (rs.next())
					{
						recordsFound = true;
						for (int index = 1; index <= columns.primaryKeyCols.size(); index++)
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object);
							index++;
						}
						if (useJdbcBatching)
							statement.addBatch();
						else
							statement.executeUpdate();
						recordCount++;
					}
					if (useJdbcBatching)
						statement.executeBatch();
				}
				destination.commit();
				log.fine(name + ": " + table + ": Records deleted so far: " + recordCount + " of "
						+ numberOfRecordsToDelete);
				if (recordCount >= numberOfRecordsToDelete || !recordsFound)
					break;
			}
			long endTime = System.currentTimeMillis();
			log.info("Finished deleting " + recordCount + " records for table " + table + " in " + (endTime - startTime)
					+ " ms");
			this.recordCount = recordCount;
			selectConnection.commit();
		}
		catch (Exception e)
		{
			log.severe("Error during data delete: " + e.toString());
			exception = new SQLException(
					"Failed to delete contents of table " + table + " with batch size " + batchSize, e);
		}
		log.fine(name + ": Finished deleting");
	}

	public SQLException getException()
	{
		return exception;
	}

	public long getRecordCount()
	{
		return recordCount;
	}

}
