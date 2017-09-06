package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class TableDeleter extends AbstractTableWorker
{
	private static final Logger log = Logger.getLogger(TableDeleter.class.getName());

	private long totalRecordCount;

	TableDeleter(String table, ConverterConfiguration config)
	{
		super(table, config);
	}

	@Override
	protected List<AbstractTablePartWorker> prepareWorkers(Connection source, Connection destination)
			throws SQLException
	{
		totalRecordCount = converterUtils.getDestinationRecordCount(destination, table);
		List<AbstractTablePartWorker> workers;
		if (totalRecordCount > 0)
		{
			if (totalRecordCount >= 10000)
			{
				workers = createWorkers(source, destination);
			}
			else
			{
				workers = new ArrayList<>(1);
				workers.add(new SingleDeleteWorker(config, table, totalRecordCount));
			}
		}
		else
		{
			workers = Collections.emptyList();
		}

		return workers;
	}

	private List<AbstractTablePartWorker> createWorkers(Connection source, Connection destination) throws SQLException
	{
		Columns columns = converterUtils.getColumns(destination, config.getCatalog(), config.getSchema(), table, true);

		String selectFormat = "SELECT $COLUMNS FROM $TABLE ORDER BY $PRIMARY_KEY LIMIT 1 OFFSET $OFFSET";
		int numberOfWorkers = config.getMaxNumberOfWorkers();
		int batchSize = converterUtils.calculateActualBatchSize(1, destination, config.getCatalog(), config.getSchema(),
				table);
		long numberOfRecordsPerWorker = totalRecordCount / numberOfWorkers;
		log.info("Deleting: Number of workers: " + numberOfWorkers + "; Batch size: " + batchSize
				+ "; Number of records per worker: " + numberOfRecordsPerWorker);
		long currentOffset = 0;
		List<AbstractTablePartWorker> workers = new ArrayList<>(numberOfWorkers);
		for (int workerNumber = 0; workerNumber < numberOfWorkers; workerNumber++)
		{
			List<Object> beginKey = new ArrayList<>(columns.getPrimaryKeyCols().size());
			List<Object> endKey = new ArrayList<>(columns.getPrimaryKeyCols().size());
			long beginKeyOffset = currentOffset;
			String select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(table + "."));
			select = select.replace("$TABLE", table);
			select = select.replace("$PRIMARY_KEY", columns.getPrimaryKeyColumns());
			select = select.replace("$OFFSET", String.valueOf(beginKeyOffset));
			try (ResultSet rsBeginKey = destination.createStatement().executeQuery(select))
			{
				while (rsBeginKey.next())
				{
					for (int i = 1; i <= columns.getPrimaryKeyCols().size(); i++)
					{
						beginKey.add(rsBeginKey.getObject(i));
					}
				}
			}

			long endKeyOffset = currentOffset + numberOfRecordsPerWorker - 1;
			if (workerNumber == (numberOfWorkers - 1))
				endKeyOffset = totalRecordCount - 1;
			select = selectFormat.replace("$COLUMNS", columns.getPrimaryKeyColumns(table + "."));
			select = select.replace("$TABLE", table);
			select = select.replace("$PRIMARY_KEY", columns.getPrimaryKeyColumns());
			select = select.replace("$OFFSET", String.valueOf(endKeyOffset));
			try (ResultSet rsEndKey = destination.createStatement().executeQuery(select))
			{
				while (rsEndKey.next())
				{
					for (int i = 1; i <= columns.getPrimaryKeyCols().size(); i++)
					{
						endKey.add(rsEndKey.getObject(i));
					}
				}
			}

			long workerRecordCount = Math.max(numberOfRecordsPerWorker, totalRecordCount - currentOffset);
			DeleteWorker worker = new DeleteWorker(config, table, columns, beginKey, endKey, workerRecordCount,
					batchSize);
			workers.add(worker);
			currentOffset = currentOffset + numberOfRecordsPerWorker;
		}
		destination.commit();

		return workers;
	}

	@Override
	public long getTotalRecordCount()
	{
		return totalRecordCount;
	}

}
