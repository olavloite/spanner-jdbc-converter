package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
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
import nl.topicus.spanner.converter.util.ConverterUtils;

public class DataConverter
{
	private static final Logger log = Logger.getLogger(DataConverter.class.getName());

	public static final String SELECT_FORMAT = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE OFFSET $OFFSET";

	private final ConverterConfiguration config;

	private final ConverterUtils converterUtils;

	static final class Table
	{
		final String schema;

		final String name;

		Table(String schema, String name)
		{
			this.schema = schema;
			this.name = name;
		}

		@Override
		public String toString()
		{
			return schema + "." + name;
		}
	}

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

	public DataConverter(ConverterConfiguration config)
	{
		this.config = config;
		this.converterUtils = new ConverterUtils(config);
	}

	public void convert(String catalog, String schema) throws SQLException
	{
		try (Connection source = DriverManager.getConnection(config.getUrlSource());
				Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			destination.setAutoCommit(false);
			source.setAutoCommit(false);
			source.setReadOnly(true);
			List<Table> tablesList = new ArrayList<>();
			try (ResultSet tables = destination.getMetaData().getTables(catalog, schema, null,
					new String[] { "TABLE" }))
			{
				while (tables.next())
				{
					String tableSchema = tables.getString("TABLE_SCHEM");
					if (!config.getDestinationDatabaseType().isSystemSchema(tableSchema))
					{
						tablesList.add(new Table(tableSchema, tables.getString("TABLE_NAME")));
					}
				}
			}
			for (Table table : tablesList)
			{
				long destinationRecordCount = getDestinationRecordCount(destination, table.name);
				if (destinationRecordCount > 0 && config.getDataConvertMode() == ConvertMode.DropAndRecreate)
				{
					deleteAll(destination, catalog, schema, table.name, destinationRecordCount);
				}
			}
			int tablesPerWorker = Math.max(Math.min(tablesList.size() / config.getNumberOfTableWorkers(), 8), 1);
			List<List<Table>> partionedTables = ConverterUtils.partition(tablesList, tablesPerWorker);

			long beginTime = System.currentTimeMillis();
			long recordCount = convertListOfTables(catalog, partionedTables);
			long endTime = System.currentTimeMillis();
			log.info("Finished converting " + recordCount + " records in " + (endTime - beginTime) + " ms");
			source.commit();
		}
	}

	private long convertListOfTables(String catalog, List<List<Table>> partionedTables) throws SQLException
	{
		long recordCount = 0;
		List<TableListWorker> workers = new ArrayList<>();
		ExecutorService service = Executors.newFixedThreadPool(config.getNumberOfTableWorkers());
		for (List<Table> tables : partionedTables)
		{
			TableListWorker worker = new TableListWorker(config, catalog, tables);
			service.submit(worker);
			workers.add(worker);
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
		for (TableListWorker worker : workers)
		{
			if (worker.getException() != null)
				throw worker.getException();
			recordCount += worker.getRecordCount();
		}
		return recordCount;
	}

	static int getSourceRecordCount(Connection source, String tableSpec) throws SQLException
	{
		try (ResultSet count = source.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableSpec))
		{
			if (count.next())
				return count.getInt(1);
		}
		return 0;
	}

	static long getDestinationRecordCount(Connection destination, String table) throws SQLException
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

	private void deleteAll(Connection destination, String catalog, String schema, String table, long recordCount)
			throws SQLException
	{
		log.info("Deleting all existing records from " + table);
		if (recordCount >= 10000)
		{
			log.info("Deleting in batches as record count is " + recordCount);
			Columns columns = TableListWorker.getColumns(destination, catalog, schema, table, true);
			destination.commit();
			long count = deleteRecordsWithWorkers(destination, columns, recordCount, catalog, schema, table);
			log.info("Finished deleting " + count + " records");
		}
		else
		{
			String sql = "delete from " + table;
			destination.createStatement().executeUpdate(sql);
			destination.commit();
		}
		log.info("Delete done on " + table);
	}

	private long deleteRecordsWithWorkers(Connection destination, Columns columns, long recordCount, String catalog,
			String schema, String table) throws SQLException
	{
		String selectFormat = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT 1 OFFSET $OFFSET";
		int numberOfWorkers = config.getMaxNumberOfWorkers();
		int batchSize = converterUtils.calculateActualBatchSize(1, destination, catalog, schema, table);
		long numberOfRecordsPerWorker = recordCount / numberOfWorkers;
		log.info("Deleting: Number of workers: " + numberOfWorkers + "; Batch size: " + batchSize
				+ "; Number of records per worker: " + numberOfRecordsPerWorker);
		long currentOffset = 0;
		List<DeleteWorker> workers = new ArrayList<>();
		for (int workerNumber = 0; workerNumber < numberOfWorkers; workerNumber++)
		{
			List<Object> beginKey = new ArrayList<>(columns.primaryKeyCols.size());
			List<Object> endKey = new ArrayList<>(columns.primaryKeyCols.size());
			long beginKeyOffset = currentOffset;
			String select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(table + "."));
			select = select.replace("$TABLE", table);
			select = select.replace("$PRIMARY_KEY", columns.getPrimaryKeyColumns());
			select = select.replace("$OFFSET", String.valueOf(beginKeyOffset));
			try (ResultSet rsBeginKey = destination.createStatement().executeQuery(select))
			{
				while (rsBeginKey.next())
				{
					for (int i = 1; i <= columns.primaryKeyCols.size(); i++)
					{
						beginKey.add(rsBeginKey.getObject(i));
					}
				}
			}

			long endKeyOffset = currentOffset + numberOfRecordsPerWorker - 1;
			if (workerNumber == (numberOfWorkers - 1))
				endKeyOffset = recordCount - 1;
			select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(table + "."));
			select = select.replace("$TABLE", table);
			select = select.replace("$PRIMARY_KEY", columns.getPrimaryKeyColumns());
			select = select.replace("$OFFSET", String.valueOf(endKeyOffset));
			try (ResultSet rsEndKey = destination.createStatement().executeQuery(select))
			{
				while (rsEndKey.next())
				{
					for (int i = 1; i <= columns.primaryKeyCols.size(); i++)
					{
						endKey.add(rsEndKey.getObject(i));
					}
				}
			}

			long workerRecordCount = Math.min(numberOfRecordsPerWorker, recordCount - currentOffset);
			DeleteWorker worker = new DeleteWorker("DeleteWorker-" + workerNumber, table, columns, beginKey, endKey,
					workerRecordCount, batchSize, config.getUrlDestination(), config.isUseJdbcBatching());
			workers.add(worker);
			currentOffset = currentOffset + numberOfRecordsPerWorker;
		}
		destination.commit();

		ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
		for (DeleteWorker worker : workers)
		{
			service.submit(worker);
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
		long deletedRecords = 0;
		for (DeleteWorker worker : workers)
		{
			if (worker.getException() != null)
				throw worker.getException();
			deletedRecords += worker.getRecordCount();
		}
		return deletedRecords;
	}

}
