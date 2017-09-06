package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.util.ConverterUtils;

public abstract class AbstractTableWorker implements Callable<ConversionResult>
{
	private static final Logger log = Logger.getLogger(AbstractTableWorker.class.getName());

	public static enum Status
	{
		CREATED, PREPARING, PREPARED, RUNNING, FINISHED
	}

	private Status status = Status.CREATED;

	protected final String table;

	protected final ConverterConfiguration config;

	protected final ConverterUtils converterUtils;

	private int numberOfWorkers;

	private Exception exception;

	private long startTime;

	private long endTime;

	private List<? extends Callable<ConversionResult>> workers;

	private ExecutorService service;

	private List<Future<ConversionResult>> futures;

	AbstractTableWorker(String table, ConverterConfiguration config)
	{
		this.table = table;
		this.config = config;
		this.converterUtils = new ConverterUtils(config);
	}

	public void prepare(Connection source, Connection destination) throws SQLException
	{
		status = Status.PREPARING;
		workers = prepareWorkers(source, destination);
		numberOfWorkers = workers.size();
		status = Status.PREPARED;
	}

	protected abstract List<? extends AbstractTablePartWorker> prepareWorkers(Connection source, Connection destination)
			throws SQLException;

	public abstract long getTotalRecordCount();

	@Override
	public ConversionResult call() throws Exception
	{
		status = Status.RUNNING;
		service = Executors.newFixedThreadPool(numberOfWorkers);
		futures = new ArrayList<>(workers.size());
		startTime = System.currentTimeMillis();
		for (Callable<ConversionResult> worker : workers)
		{
			futures.add(service.submit(worker));
		}
		service.shutdown();
		try
		{
			service.awaitTermination(config.getTableWorkerMaxWaitInMinutes(), TimeUnit.MINUTES);
		}
		catch (InterruptedException e)
		{
			service.shutdownNow();
			log.severe("Error while waiting for upload workers to finish: " + e.getMessage());
			exception = e;
		}
		endTime = System.currentTimeMillis();
		status = Status.FINISHED;
		return ConversionResult.collect(futures, startTime, endTime, exception);
	}

	public Status getStatus()
	{
		return status;
	}

}
