package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.Callable;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class TablePreparer implements Callable<ConversionResult>
{
	private final ConverterConfiguration config;

	private final AbstractTableWorker worker;

	public TablePreparer(ConverterConfiguration config, AbstractTableWorker worker)
	{
		this.config = config;
		this.worker = worker;
	}

	@Override
	public ConversionResult call() throws Exception
	{
		long startTime = System.currentTimeMillis();
		try (Connection source = DriverManager.getConnection(config.getUrlSource());
				Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			worker.prepare(source, destination);
		}
		long endTime = System.currentTimeMillis();
		return new ConversionResult(worker.getTotalRecordCount(), 0, startTime, endTime);
	}

}
