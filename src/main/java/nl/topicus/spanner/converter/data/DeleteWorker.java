package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class DeleteWorker extends AbstractTablePartWorker
{
	private static final Logger log = Logger.getLogger(DeleteWorker.class.getName());

	private String selectFormat = "SELECT $COLUMNS FROM $TABLE WHERE $WHERE_CLAUSE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE";

	private Columns columns;

	private List<Object> beginKey;

	private List<Object> endKey;

	private long numberOfRecordsToDelete;

	private int batchSize;

	private long recordCount;

	DeleteWorker(ConverterConfiguration config, String table, Columns columns, List<Object> beginKey,
			List<Object> endKey, long numberOfRecordsToDelete, int batchSize)
	{
		super(config, table);
		this.columns = columns;
		this.beginKey = beginKey;
		this.endKey = endKey;
		this.numberOfRecordsToDelete = numberOfRecordsToDelete;
		this.batchSize = batchSize;
	}

	@Override
	public void run() throws SQLException
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination());
				Connection selectConnection = DriverManager.getConnection(config.getUrlDestination()))
		{
			log.fine(sourceTable + ": Starting deleting " + numberOfRecordsToDelete + " records");
			selectConnection.setReadOnly(true);
			destination.setAutoCommit(false);

			boolean first = true;
			String sql = "DELETE FROM " + sourceTable + " WHERE ";
			for (String col : columns.getPrimaryKeyCols())
			{
				if (!first)
					sql = sql + " AND ";
				sql = sql + col + "=?";
				first = false;
			}
			PreparedStatement statement = destination.prepareStatement(sql);

			int limit = batchSize;
			String select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(sourceTable + "."));
			select = select.replace("$TABLE", sourceTable);
			select = select.replace("$WHERE_CLAUSE", columns.getPrimaryKeyColumnsWhereClause(sourceTable + "."));
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
						for (int index = 1; index <= columns.getPrimaryKeyCols().size(); index++)
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object);
							index++;
						}
						if (config.isUseJdbcBatching())
							statement.addBatch();
						else
							statement.executeUpdate();
						recordCount++;
					}
					if (config.isUseJdbcBatching())
						statement.executeBatch();
				}
				destination.commit();
				log.fine(sourceTable + ": Records deleted so far: " + recordCount + " of " + numberOfRecordsToDelete);
				if (recordCount >= numberOfRecordsToDelete || !recordsFound)
					break;
			}
			this.recordCount = recordCount;
			selectConnection.commit();
		}
		log.fine(sourceTable + ": Finished deleting");
	}

	@Override
	public long getRecordCount()
	{
		return recordCount;
	}

	@Override
	protected long getByteCount()
	{
		return 0;
	}

}
