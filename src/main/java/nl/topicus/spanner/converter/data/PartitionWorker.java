package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class PartitionWorker extends AbstractTablePartWorker
{
	private static final Logger log = Logger.getLogger(PartitionWorker.class.getName());

	private final ResultSet resultSet;

	public PartitionWorker(String name, ConverterConfiguration config, ResultSet resultSet, String sourceTable,
			String destinationTable, Columns insertCols)
	{
		super(config, sourceTable, destinationTable, insertCols);
		this.resultSet = resultSet;
	}

	@Override
	protected void run() throws Exception
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			log.fine(sourceTable + ": Starting copying partition");
			destination.setAutoCommit(false);

			PreparedStatement insertStatement = createInsertStatement(destination);
			copyResultSet(resultSet, insertStatement);
			destination.commit();
		}
		log.fine(sourceTable + ": Finished copying " + getRecordCount() + " records");
	}

}
