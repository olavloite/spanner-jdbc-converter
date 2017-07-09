package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class DataConverter
{
	private static final Logger log = Logger.getLogger(DataConverter.class.getName());

	private final Connection source;

	private final Connection destination;

	private final ConverterConfiguration config;

	private String selectFormat = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE OFFSET $OFFSET";

	static final class Columns
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

		String getColumnParameters()
		{
			String[] params = new String[columnNames.size()];
			Arrays.fill(params, "?");
			return String.join(", ", params);
		}
	}

	public DataConverter(Connection source, Connection destination, ConverterConfiguration config)
	{
		this.source = source;
		this.destination = destination;
		this.config = config;
	}

	public void convert(String catalog, String schema) throws SQLException
	{
		int batchSize = config.getBatchSize();
		destination.setAutoCommit(false);
		source.setAutoCommit(false);
		source.setReadOnly(true);
		try (ResultSet tables = destination.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tables.next())
			{
				String tableSchema = tables.getString("TABLE_SCHEM");
				if (!config.getDestinationDatabaseType().isSystemSchema(tableSchema))
				{
					String table = tables.getString("TABLE_NAME");
					// Check whether the destination table is empty.
					int destinationRecordCount = getDestinationRecordCount(table);
					if (destinationRecordCount == 0 || config.getDataConvertMode() == ConvertMode.DropAndRecreate)
					{
						if (destinationRecordCount > 0)
						{
							deleteAll(table);
						}
						int sourceRecordCount = getSourceRecordCount(getTableSpec(catalog, tableSchema, table));
						if (sourceRecordCount > batchSize)
						{
							convertTableWithWorkers(catalog, tableSchema, table);
						}
						else
						{
							convertTable(catalog, tableSchema, table);
						}
					}
					else
					{
						if (config.getDataConvertMode() == ConvertMode.ThrowExceptionIfExists)
							throw new IllegalStateException("Table " + table + " is not empty");
						else if (config.getDataConvertMode() == ConvertMode.SkipExisting)
							log.info("Skipping data copy for table " + table);
					}
				}
			}
		}
		source.commit();
	}

	private void convertTable(String catalog, String schema, String table) throws SQLException
	{
		int batchSize = config.getBatchSize();
		String tableSpec = getTableSpec(catalog, schema, table);

		Columns insertCols = getColumns(catalog, schema, table, false);
		if (insertCols.primaryKeyCols.isEmpty())
		{
			log.warning("Table " + tableSpec + " does not have a primary key. No data will be copied.");
			return;
		}
		String sql = "INSERT INTO " + table + " (" + insertCols.getColumnNames() + ") VALUES \n";
		sql = sql + "(" + insertCols.getColumnParameters() + ")";
		PreparedStatement statement = destination.prepareStatement(sql);
		log.info("About to copy data from table " + tableSpec);

		int currentOffset = 0;
		int recordCount = 0;
		int totalRecordCount = getSourceRecordCount(tableSpec);
		log.info(tableSpec + ": Records to be copied: " + totalRecordCount);

		Columns selectCols = getColumns(catalog, schema, table, true);
		while (true)
		{
			String select = selectFormat.replace("$COLUMNS", selectCols.getColumnNames());
			select = select.replace("$TABLE", tableSpec);
			select = select.replace("$PRIMARY_KEY", selectCols.getPrimaryKeyColumns());
			select = select.replace("$BATCH_SIZE", String.valueOf(batchSize));
			select = select.replace("$OFFSET", String.valueOf(currentOffset));
			try (ResultSet rs = source.createStatement().executeQuery(select))
			{
				while (rs.next())
				{
					int index = 1;
					for (Integer type : insertCols.columnTypes)
					{
						Object object = rs.getObject(index);
						statement.setObject(index, object, type);
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
			log.info(tableSpec + ": Records copied so far: " + recordCount + " of " + totalRecordCount);
			currentOffset = currentOffset + batchSize;
			if (recordCount >= totalRecordCount)
				break;
		}
		log.info(tableSpec + ": Finished copying: " + recordCount + " of " + totalRecordCount);
	}

	private void convertTableWithWorkers(String catalog, String schema, String table) throws SQLException
	{
		String tableSpec = getTableSpec(catalog, schema, table);
		Columns insertCols = getColumns(catalog, schema, table, false);
		Columns selectCols = getColumns(catalog, schema, table, true);
		if (insertCols.primaryKeyCols.isEmpty())
		{
			log.warning("Table " + tableSpec + " does not have a primary key. No data will be copied.");
			return;
		}
		log.info("About to copy data from table " + tableSpec);

		int batchSize = config.getBatchSize();
		int totalRecordCount = getSourceRecordCount(tableSpec);
		int numberOfWorkers = calculateNumberOfWorkers(totalRecordCount);
		int numberOfRecordsPerWorker = totalRecordCount / numberOfWorkers;
		if (totalRecordCount % numberOfWorkers > 0)
			numberOfRecordsPerWorker++;
		int currentOffset = 0;
		ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
		for (int workerNumber = 0; workerNumber < numberOfWorkers; workerNumber++)
		{
			int workerRecordCount = Math.min(numberOfRecordsPerWorker, totalRecordCount - currentOffset);
			UploadWorker worker = new UploadWorker("UploadWorker-" + workerNumber, selectFormat, tableSpec, table,
					insertCols, selectCols, currentOffset, workerRecordCount, batchSize, config.getUrlSource(),
					config.getUrlDestination(), config.isUseJdbcBatching());
			service.submit(worker);
			currentOffset = currentOffset + numberOfRecordsPerWorker;
		}
		service.shutdown();
		try
		{
			service.awaitTermination(config.getUploadWorkerMaxWaitInMinutes(), TimeUnit.MINUTES);
		}
		catch (InterruptedException e)
		{
			log.severe("Error while waiting for workers to finish: " + e.getMessage());
			throw new RuntimeException(e);
		}

	}

	private int calculateNumberOfWorkers(int totalRecordCount)
	{
		int res = totalRecordCount / config.getBatchSize() + 1;
		return Math.min(res, config.getMaxNumberOfWorkers());
	}

	private Columns getColumns(String catalog, String schema, String table, boolean forSelect) throws SQLException
	{
		Columns res = new Columns();
		try (ResultSet columns = destination.getMetaData().getColumns(catalog, schema, table, null))
		{
			while (columns.next())
			{
				// When doing a select and a column is named the same as the
				// table, Cloud Spanner will misinterpret the query. In those
				// cases, the column name will be prefixed by the table name
				res.columnNames.add(forSelect && columns.getString("COLUMN_NAME").equalsIgnoreCase(table) ? table + "."
						+ columns.getString("COLUMN_NAME") : columns.getString("COLUMN_NAME"));
				res.columnTypes.add(columns.getInt("DATA_TYPE"));
			}
		}
		try (ResultSet keys = destination.getMetaData().getPrimaryKeys(catalog, schema, table))
		{
			while (keys.next())
			{
				res.primaryKeyCols.add(keys.getString("COLUMN_NAME"));
			}
		}
		return res;
	}

	private String getTableSpec(String catalog, String schema, String table)
	{
		String tableSpec = "";
		if (catalog != null && !"".equals(catalog))
			tableSpec = tableSpec + catalog + ".";
		if (schema != null && !"".equals(schema) && !"public".equals(schema))
			tableSpec = tableSpec + schema + ".";
		tableSpec = tableSpec + table;

		return tableSpec;
	}

	private int getSourceRecordCount(String tableSpec) throws SQLException
	{
		try (ResultSet count = source.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableSpec))
		{
			if (count.next())
				return count.getInt(1);
		}
		return 0;
	}

	private int getDestinationRecordCount(String table) throws SQLException
	{
		String sql = "select count(*) from " + table;
		try (ResultSet rs = destination.createStatement().executeQuery(sql))
		{
			if (rs.next())
				return rs.getInt(1);
		}
		finally
		{
			destination.commit();
		}
		return 0;
	}

	private void deleteAll(String table) throws SQLException
	{
		log.info("Deleting all existing records from " + table);
		String sql = "delete from " + table;
		destination.createStatement().executeUpdate(sql);
		destination.commit();
		log.info("Delete done on " + table);
	}

	public String getSelectFormat()
	{
		return selectFormat;
	}

	public void setSelectFormat(String selectFormat)
	{
		this.selectFormat = selectFormat;
	}

}
