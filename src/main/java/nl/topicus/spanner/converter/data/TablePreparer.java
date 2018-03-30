package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.util.concurrent.Callable;

public class TablePreparer implements Callable<ConversionResult>
{
	private final AbstractTableWorker worker;

	private final Connection source;

	private final Connection destination;

	public TablePreparer(AbstractTableWorker worker, Connection source, Connection destination)
	{
		this.worker = worker;
		this.source = source;
		this.destination = destination;
	}

	@Override
	public ConversionResult call() throws Exception
	{
		long startTime = System.currentTimeMillis();
		worker.prepare(source, destination);
		long endTime = System.currentTimeMillis();
		return new ConversionResult(worker.getTotalRecordCount(), 0, startTime, endTime);
	}

}
