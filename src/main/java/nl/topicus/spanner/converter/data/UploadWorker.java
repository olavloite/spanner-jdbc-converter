package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class UploadWorker extends AbstractTablePartWorker
{
	private static final Logger log = Logger.getLogger(UploadWorker.class.getName());

	private String selectFormat;

	private Columns selectCols;

	private long beginOffset;

	private int batchSize;

	private final long numberOfRecordsToCopy;

	UploadWorker(String name, ConverterConfiguration config, String selectFormat, String sourceTable,
			String destinationTable, Columns insertCols, Columns selectCols, long beginOffset,
			long numberOfRecordsToCopy, int batchSize)
	{
		super(config, sourceTable, destinationTable, insertCols);
		this.selectFormat = selectFormat;
		this.selectCols = selectCols;
		this.beginOffset = beginOffset;
		this.numberOfRecordsToCopy = numberOfRecordsToCopy;
		this.batchSize = batchSize;
	}

	@Override
	public void run() throws SQLException
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination());
				Connection source = DriverManager.getConnection(config.getUrlSource()))
		{
			log.fine(sourceTable + ": Starting copying " + numberOfRecordsToCopy + " records");
			destination.setAutoCommit(false);

			long lastRecord = beginOffset + numberOfRecordsToCopy;
			long currentOffset = beginOffset;
			PreparedStatement insertStatement = createInsertStatement(destination);
			while (true)
			{
				long limit = Math.min(batchSize, lastRecord - currentOffset);
				String select = selectFormat.replace("$COLUMNS", selectCols.getColumnNames());
				select = select.replace("$TABLE", sourceTable);
				select = select.replace("$PRIMARY_KEY", selectCols.getPrimaryKeyColumns());
				select = select.replace("$BATCH_SIZE", String.valueOf(limit));
				select = select.replace("$OFFSET", String.valueOf(currentOffset));
				try (ResultSet rs = source.createStatement().executeQuery(select))
				{
					copyResultSet(rs, insertStatement);
				}
				catch (SQLException e)
				{
					setException(e);
					throw e;
				}
				destination.commit();
				log.info(sourceTable + ": Records copied so far: " + getRecordCount() + " of " + numberOfRecordsToCopy);
				currentOffset = currentOffset + batchSize;
				if (getRecordCount() >= numberOfRecordsToCopy)
					break;
			}
		}
		catch (SQLException e)
		{
			setException(e);
			throw e;
		}
		log.fine(sourceTable + ": Finished copying " + getRecordCount() + " records");
	}

}
