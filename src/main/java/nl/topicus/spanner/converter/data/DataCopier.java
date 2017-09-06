package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class DataCopier
{
	private static final Logger log = Logger.getLogger(DataCopier.class.getName());

	public static final String SELECT_FORMAT = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE OFFSET $OFFSET";

	private final ConverterConfiguration config;

	private List<String> tables = new ArrayList<>();

	private List<TableDeleter> deleters = new ArrayList<>();

	private List<TablePreparer> deletePreparers = new ArrayList<>();

	private List<TableWorker> copiers = new ArrayList<>();

	private List<TablePreparer> copyPreparers = new ArrayList<>();

	public DataCopier(ConverterConfiguration config)
	{
		this.config = config;
	}

	public void convert() throws SQLException
	{
		init();
		deleteData();
		copyData();
	}

	private void init() throws SQLException
	{
		initTables();
	}

	private void deleteData() throws SQLException
	{
		if (config.getDataConvertMode() == ConvertMode.DropAndRecreate)
		{

			createTableDeleters();
			ConversionResult prepare = runWorkers(deletePreparers);
			log.info("Preparing delete finished with result: " + prepare.toString());
			ConversionResult run = runWorkers(deleters);
			log.info("Running delete finished with result: " + run.toString());
		}
	}

	private void copyData() throws SQLException
	{
		createTableWorkers();
		ConversionResult prepare = runWorkers(copyPreparers);
		log.info("Preparing copy finished with result: " + prepare.toString());
		ConversionResult run = runWorkers(copiers);
		log.info("Running copy finished with result: " + run.toString());
	}

	private void initTables() throws SQLException
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			try (ResultSet rs = destination.getMetaData().getTables(config.getCatalog(), config.getSchema(), null,
					new String[] { "TABLE" }))
			{
				while (rs.next())
				{
					String tableSchema = rs.getString("TABLE_SCHEM");
					if (!config.getDestinationDatabaseType().isSystemSchema(tableSchema))
					{
						tables.add(rs.getString("TABLE_NAME"));
					}
				}
			}
		}
	}

	private void createTableDeleters()
	{
		for (String table : tables)
		{
			TableDeleter worker = new TableDeleter(table, config);
			deleters.add(worker);
			deletePreparers.add(new TablePreparer(config, worker));
		}
	}

	private void createTableWorkers()
	{
		for (String table : tables)
		{
			TableWorker worker = new TableWorker(table, config);
			copiers.add(worker);
			copyPreparers.add(new TablePreparer(config, worker));
		}
	}

	private ConversionResult runWorkers(List<? extends Callable<ConversionResult>> callables)
	{
		Exception exception = null;
		ExecutorService service = Executors.newFixedThreadPool(config.getNumberOfTableWorkers());
		List<Future<ConversionResult>> futures = new ArrayList<>(callables.size());
		long startTime = System.currentTimeMillis();
		for (Callable<ConversionResult> callable : callables)
		{
			futures.add(service.submit(callable));
		}
		service.shutdown();
		try
		{
			service.awaitTermination(config.getTableWorkerMaxWaitInMinutes(), TimeUnit.MINUTES);
		}
		catch (InterruptedException e)
		{
			exception = e;
			service.shutdownNow();
			log.severe("Error while waiting for workers to finish: " + e.getMessage());
		}
		long endTime = System.currentTimeMillis();
		return ConversionResult.collect(futures, startTime, endTime, exception);
	}

}
