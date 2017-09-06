package nl.topicus.spanner.converter.data;

import java.util.concurrent.Callable;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public abstract class AbstractTablePartWorker implements Callable<ConversionResult>
{
	protected final ConverterConfiguration config;

	protected final String table;

	protected final long totalRecordCount;

	AbstractTablePartWorker(ConverterConfiguration config, String table, long totalRecordCount)
	{
		this.config = config;
		this.table = table;
		this.totalRecordCount = totalRecordCount;
	}

	@Override
	public ConversionResult call() throws Exception
	{
		Exception exception = null;
		long startTime = System.currentTimeMillis();
		try
		{
			run();
		}
		catch (Exception e)
		{
			exception = e;
		}
		long endTime = System.currentTimeMillis();
		return new ConversionResult(totalRecordCount, getByteCount(), startTime, endTime, exception);
	}

	protected abstract void run() throws Exception;

	protected abstract long getByteCount();

}
