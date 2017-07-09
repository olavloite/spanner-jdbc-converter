package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.data.DataConverter.Columns;

public class UploadWorker implements Runnable
{
	private static final Logger log = Logger.getLogger(UploadWorker.class.getName());

	private final String name;

	private String selectFormat;

	private String sourceTable;

	private String destinationTable;

	private Columns insertCols;

	private Columns selectCols;

	private int beginOffset;

	private int numberOfRecordsToCopy;

	private int batchSize;

	private String urlSource;

	private String urlDestination;

	private boolean useJdbcBatching;

	UploadWorker(String name, String selectFormat, String sourceTable, String destinationTable, Columns insertCols,
			Columns selectCols, int beginOffset, int numberOfRecordsToCopy, int batchSize, String urlSource,
			String urlDestination, boolean useJdbcBatching)
	{
		this.name = name;
		this.selectFormat = selectFormat;
		this.sourceTable = sourceTable;
		this.destinationTable = destinationTable;
		this.insertCols = insertCols;
		this.selectCols = selectCols;
		this.beginOffset = beginOffset;
		this.numberOfRecordsToCopy = numberOfRecordsToCopy;
		this.batchSize = batchSize;
		this.urlSource = urlSource;
		this.urlDestination = urlDestination;
		this.useJdbcBatching = useJdbcBatching;
	}

	@Override
	public void run()
	{
		try (Connection source = DriverManager.getConnection(urlSource);
				Connection destination = DriverManager.getConnection(urlDestination))
		{
			log.info(name + ": " + sourceTable + ": Starting copying " + numberOfRecordsToCopy + " records");

			destination.setAutoCommit(false);
			String sql = "INSERT INTO " + destinationTable + " (" + insertCols.getColumnNames() + ") VALUES \n";
			sql = sql + "(" + insertCols.getColumnParameters() + ")";
			PreparedStatement statement = destination.prepareStatement(sql);

			int lastRecord = beginOffset + numberOfRecordsToCopy;
			int recordCount = 0;
			int currentOffset = beginOffset;
			while (true)
			{
				int limit = Math.min(batchSize, lastRecord - currentOffset);
				String select = selectFormat.replace("$COLUMNS", selectCols.getColumnNames());
				select = select.replace("$TABLE", sourceTable);
				select = select.replace("$PRIMARY_KEY", selectCols.getPrimaryKeyColumns());
				select = select.replace("$BATCH_SIZE", String.valueOf(limit));
				select = select.replace("$OFFSET", String.valueOf(currentOffset));
				try (ResultSet rs = source.createStatement().executeQuery(select))
				{
					while (rs.next())
					{
						int index = 1;
						for (Integer type : insertCols.columnTypes)
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object, type);
							index++;
						}
						if (useJdbcBatching)
							statement.addBatch();
						else
							statement.executeUpdate();
						recordCount++;
					}
					if (useJdbcBatching)
						statement.executeBatch();
				}
				destination.commit();
				log.info(name + ": " + sourceTable + ": Records copied so far: " + recordCount + " of "
						+ numberOfRecordsToCopy);
				currentOffset = currentOffset + batchSize;
				if (recordCount >= numberOfRecordsToCopy)
					break;
			}
		}
		catch (SQLException e)
		{
			log.severe("Error during data copy: " + e.getMessage());
			throw new RuntimeException(e);
		}
		log.info(name + ": Finished copying");
	}

}
