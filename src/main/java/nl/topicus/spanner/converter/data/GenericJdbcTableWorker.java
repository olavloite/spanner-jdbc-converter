package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class GenericJdbcTableWorker extends AbstractTableWorker
{
	private static final Logger log = Logger.getLogger(GenericJdbcTableWorker.class.getName());

	private long totalRecordCount;

	GenericJdbcTableWorker(String table, ConverterConfiguration config)
	{
		super(table, config);
	}

	@Override
	protected List<AbstractTablePartWorker> prepareWorkers(Connection source, Connection destination)
			throws SQLException
	{
		String tableSpec = converterUtils.getTableSpec(config.getCatalog(), config.getSchema(), table);
		Columns insertCols = converterUtils.getColumns(destination, config.getCatalog(), config.getSchema(), table,
				false);
		Columns selectCols = converterUtils.getColumns(destination, config.getCatalog(), config.getSchema(), table,
				true);
		if (insertCols.getPrimaryKeyCols().isEmpty())
		{
			log.warning("Table " + tableSpec + " does not have a primary key. No data will be copied.");
			return Collections.emptyList();
		}

		int batchSize = converterUtils.calculateActualBatchSize(insertCols.getColumns().size(), destination,
				config.getCatalog(), config.getSchema(), table);
		totalRecordCount = converterUtils.getSourceRecordCount(source, tableSpec);

		int numberOfWorkers = calculateNumberOfWorkers(totalRecordCount, batchSize);
		log.info("About to copy " + totalRecordCount + " records from table " + tableSpec + " with batch size "
				+ batchSize + " and " + numberOfWorkers + " workers");
		long numberOfRecordsPerWorker = totalRecordCount / numberOfWorkers;
		if (totalRecordCount % numberOfWorkers > 0)
			numberOfRecordsPerWorker++;
		long currentOffset = 0;

		List<AbstractTablePartWorker> workers = new ArrayList<>(numberOfWorkers);
		for (int workerNumber = 0; workerNumber < numberOfWorkers; workerNumber++)
		{
			long workerRecordCount = Math.min(numberOfRecordsPerWorker, totalRecordCount - currentOffset);
			UploadWorker worker = new UploadWorker("UploadWorker-" + workerNumber, config, DataCopier.SELECT_FORMAT,
					tableSpec, table, insertCols, selectCols, currentOffset, workerRecordCount, batchSize);
			workers.add(worker);
			currentOffset = currentOffset + numberOfRecordsPerWorker;
		}
		return workers;
	}

	@Override
	public long getTotalRecordCount()
	{
		return totalRecordCount;
	}

	private int calculateNumberOfWorkers(long totalRecordCount, int batchSize)
	{
		long res = totalRecordCount / batchSize + 1;
		return (int) Math.min(res, config.getMaxNumberOfWorkers());
	}

}
