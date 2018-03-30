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

import nl.topicus.jdbc.ICloudSpannerConnection;
import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration.DatabaseType;

public class DataCopier
{
	private static final Logger log = Logger.getLogger(DataCopier.class.getName());

	public static final String SELECT_FORMAT = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT $BATCH_SIZE OFFSET $OFFSET";
	public static final String CLOUD_SPANNER_SELECT_FORMAT = "SELECT $COLUMNS FROM $TABLE";

	private final ConverterConfiguration config;

	private List<String> tables = new ArrayList<>();

	private List<TableDeleter> deleters = new ArrayList<>();

	private List<TablePreparer> deletePreparers = new ArrayList<>();

	private List<AbstractTableWorker> copiers = new ArrayList<>();

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
			try (Connection source = DriverManager.getConnection(config.getUrlSource());
					Connection destination = DriverManager.getConnection(config.getUrlDestination()))
			{
				createTableDeleters(source, destination);
				ConversionResult prepare = runWorkers(deletePreparers);
				log.info("Preparing delete finished with result: " + prepare.toString());
				ConversionResult run = runWorkers(deleters);
				log.info("Running delete finished with result: " + run.toString());
			}
		}
	}

	private void copyData() throws SQLException
	{
		try (Connection source = DriverManager.getConnection(config.getUrlSource());
				Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			prepareSourceConnection(source);
			createTableWorkers(source, destination);
			ConversionResult prepare = runWorkers(copyPreparers);
			log.info("Preparing copy finished with result: " + prepare.toString());
			ConversionResult run = runWorkers(copiers);
			log.info("Running copy finished with result: " + run.toString());
		}
	}

	private void prepareSourceConnection(Connection source) throws SQLException
	{
		if (source.isWrapperFor(ICloudSpannerConnection.class))
		{
			ICloudSpannerConnection con = source.unwrap(ICloudSpannerConnection.class);
			// Make sure no transaction is running
			if (!con.isBatchReadOnly())
			{
				if (con.getAutoCommit())
				{
					con.setAutoCommit(false);
				}
				else
				{
					con.commit();
				}
				con.setBatchReadOnly(true);
			}
		}
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
					if (!config.getDestinationDatabaseType().isSystemSchema(tableSchema)
							&& tableExistsInSource(rs.getString("TABLE_NAME")))
					{
						tables.add(rs.getString("TABLE_NAME"));
					}
				}
			}
		}
	}

	private boolean tableExistsInSource(String table) throws SQLException
	{
		try (Connection source = DriverManager.getConnection(config.getUrlSource()))
		{
			try (ResultSet rs = source.getMetaData().getTables(config.getCatalog(), config.getSchema(), table,
					new String[] { "TABLE" }))
			{
				return rs.next();
			}
		}
	}

	private void createTableDeleters(Connection source, Connection destination) throws SQLException
	{
		for (String table : tables)
		{
			TableDeleter worker = new TableDeleter(table, config);
			deleters.add(worker);
			deletePreparers.add(new TablePreparer(worker, source, destination));
		}
	}

	private void createTableWorkers(Connection source, Connection destination) throws SQLException
	{
		for (String table : tables)
		{
			AbstractTableWorker worker = createTableWorker(table, config);
			copiers.add(worker);
			copyPreparers.add(new TablePreparer(worker, source, destination));
		}
	}

	private AbstractTableWorker createTableWorker(String table, ConverterConfiguration config)
	{
		if (config.getSourceDatabaseType() == DatabaseType.CloudSpanner)
			return new CloudSpannerTableWorker(table, config);
		return new GenericJdbcTableWorker(table, config);
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
