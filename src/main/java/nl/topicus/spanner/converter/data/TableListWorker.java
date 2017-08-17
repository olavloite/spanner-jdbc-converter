package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.data.DataConverter.Columns;
import nl.topicus.spanner.converter.data.DataConverter.Table;
import nl.topicus.spanner.converter.util.ConverterUtils;

public class TableListWorker implements Runnable
{
	private static final Logger log = Logger.getLogger(TableListWorker.class.getName());

	private final ConverterConfiguration config;

	private final ConverterUtils converterUtils;

	private final String catalog;

	private final List<Table> tablesList;

	private SQLException exception;

	private long recordCount;

	private long byteCount;

	TableListWorker(ConverterConfiguration config, String catalog, List<Table> tablesList)
	{
		this.config = config;
		this.catalog = catalog;
		this.tablesList = tablesList;
		this.converterUtils = new ConverterUtils(config);
	}

	@Override
	public void run()
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination());
				Connection source = DriverManager.getConnection(config.getUrlSource()))
		{
			for (Table table : tablesList)
			{
				long destinationRecordCount = DataConverter.getDestinationRecordCount(destination, table.name);
				if (destinationRecordCount == 0)
				{
					convertTableWithWorkers(source, destination, catalog, table.schema, table.name);
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
		catch (SQLException e)
		{
			log.severe("Error during data copy: " + e.getMessage());
			exception = e;
		}
	}

	private void convertTableWithWorkers(Connection source, Connection destination, String catalog, String schema,
			String table) throws SQLException
	{
		String tableSpec = getTableSpec(catalog, schema, table);
		Columns insertCols = getColumns(destination, catalog, schema, table, false);
		Columns selectCols = getColumns(destination, catalog, schema, table, true);
		if (insertCols.primaryKeyCols.isEmpty())
		{
			log.warning("Table " + tableSpec + " does not have a primary key. No data will be copied.");
			return;
		}

		int batchSize = converterUtils.calculateActualBatchSize(insertCols.columnNames.size(), destination, catalog,
				schema, table);
		int totalRecordCount = DataConverter.getSourceRecordCount(source, tableSpec);
		int numberOfWorkers = calculateNumberOfWorkers(totalRecordCount, batchSize);
		log.info("About to copy " + totalRecordCount + " records from table " + tableSpec + " with batch size "
				+ batchSize + " and " + numberOfWorkers + " workers");
		int numberOfRecordsPerWorker = totalRecordCount / numberOfWorkers;
		if (totalRecordCount % numberOfWorkers > 0)
			numberOfRecordsPerWorker++;
		int currentOffset = 0;
		ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
		List<UploadWorker> workers = new ArrayList<>();
		for (int workerNumber = 0; workerNumber < numberOfWorkers; workerNumber++)
		{
			int workerRecordCount = Math.min(numberOfRecordsPerWorker, totalRecordCount - currentOffset);
			UploadWorker worker = new UploadWorker("UploadWorker-" + workerNumber, config, DataConverter.SELECT_FORMAT,
					catalog, schema, tableSpec, table, insertCols, selectCols, currentOffset, workerRecordCount,
					batchSize);
			service.submit(worker);
			workers.add(worker);
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
		for (UploadWorker worker : workers)
		{
			if (worker.getException() != null)
				throw worker.getException();
			recordCount += worker.getRecordCount();
			byteCount += worker.getByteCount();
		}
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

	static Columns getColumns(Connection destination, String catalog, String schema, String table, boolean forSelect)
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
				res.columnNames.add(forSelect && columns.getString("COLUMN_NAME").equalsIgnoreCase(table)
						? table + "." + columns.getString("COLUMN_NAME") : columns.getString("COLUMN_NAME"));
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

	private int calculateNumberOfWorkers(int totalRecordCount, int batchSize)
	{
		int res = totalRecordCount / batchSize + 1;
		return Math.min(res, config.getMaxNumberOfWorkers());
	}

	public SQLException getException()
	{
		return exception;
	}

	public long getRecordCount()
	{
		return recordCount;
	}

	public long getByteCount()
	{
		return byteCount;
	}

}
