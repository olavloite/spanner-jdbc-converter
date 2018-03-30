package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import nl.topicus.jdbc.ICloudSpannerConnection;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class CloudSpannerTableWorker extends AbstractTableWorker
{
	private static final Logger log = Logger.getLogger(CloudSpannerTableWorker.class.getName());

	private long totalRecordCount;

	CloudSpannerTableWorker(String table, ConverterConfiguration config)
	{
		super(table, config);
	}

	@Override
	protected List<AbstractTablePartWorker> prepareWorkers(Connection source, Connection destination)
			throws SQLException
	{
		if (!source.isWrapperFor(ICloudSpannerConnection.class))
		{
			throw new IllegalArgumentException("The given source connection is not a ICloudSpannerConnection");
		}
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
		totalRecordCount = converterUtils.getSourceRecordCount(source, tableSpec);
		// Get partitioned result sets
		String select = DataCopier.CLOUD_SPANNER_SELECT_FORMAT.replace("$COLUMNS", selectCols.getColumnNames());
		select = select.replace("$TABLE", tableSpec);

		List<AbstractTablePartWorker> workers = new ArrayList<>(config.getMaxNumberOfWorkers());
		Statement statement = source.createStatement();
		boolean hasResults = statement.execute(select);
		int workerNumber = 0;
		while (hasResults)
		{
			ResultSet rs = statement.getResultSet();
			PartitionWorker worker = new PartitionWorker("PartionWorker-" + workerNumber, config, rs, tableSpec, table,
					insertCols);
			workers.add(worker);
			hasResults = statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
			workerNumber++;
		}
		log.info("About to copy " + totalRecordCount + " records from table " + tableSpec + " with " + workers.size()
				+ " workers");
		return workers;
	}

	@Override
	public long getTotalRecordCount()
	{
		return totalRecordCount;
	}

}
