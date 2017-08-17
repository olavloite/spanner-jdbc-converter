package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.data.DataConverter.Columns;
import nl.topicus.spanner.converter.util.ConverterUtils;

public class UploadWorker implements Runnable
{
	private static final Logger log = Logger.getLogger(UploadWorker.class.getName());

	private final ConverterUtils converterUtils;

	private final ConverterConfiguration config;

	private final String name;

	private String selectFormat;

	private String catalog;

	private String schema;

	private String sourceTable;

	private String destinationTable;

	private Columns insertCols;

	private Columns selectCols;

	private int beginOffset;

	private int numberOfRecordsToCopy;

	private int batchSize;

	private SQLException exception;

	private long recordCount;

	private long byteCount;

	UploadWorker(String name, ConverterConfiguration config, String selectFormat, String catalog, String schema,
			String sourceTable, String destinationTable, Columns insertCols, Columns selectCols, int beginOffset,
			int numberOfRecordsToCopy, int batchSize)
	{
		this.converterUtils = new ConverterUtils(config);
		this.config = config;
		this.name = name;
		this.selectFormat = selectFormat;
		this.catalog = catalog;
		this.schema = schema;
		this.sourceTable = sourceTable;
		this.destinationTable = destinationTable;
		this.insertCols = insertCols;
		this.selectCols = selectCols;
		this.beginOffset = beginOffset;
		this.numberOfRecordsToCopy = numberOfRecordsToCopy;
		this.batchSize = batchSize;
	}

	@Override
	public void run()
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination());
				Connection source = DriverManager.getConnection(config.getUrlSource()))
		{
			long startTime = System.currentTimeMillis();
			log.fine(name + ": " + sourceTable + ": Starting copying " + numberOfRecordsToCopy + " records");

			destination.setAutoCommit(false);
			String sql = "INSERT INTO " + destinationTable + " (" + insertCols.getColumnNames() + ") VALUES \n";
			sql = sql + "(" + insertCols.getColumnParameters() + ")";
			PreparedStatement statement = destination.prepareStatement(sql);

			int rowSize = converterUtils.getRowSize(destination, catalog, schema, destinationTable);
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
						if (config.isUseJdbcBatching())
							statement.addBatch();
						else
							statement.executeUpdate();
						recordCount++;
						byteCount += rowSize;
					}
					if (config.isUseJdbcBatching())
						statement.executeBatch();
				}
				destination.commit();
				log.fine(name + ": " + sourceTable + ": Records copied so far: " + recordCount + " of "
						+ numberOfRecordsToCopy);
				currentOffset = currentOffset + batchSize;
				if (recordCount >= numberOfRecordsToCopy)
					break;
			}
			long endTime = System.currentTimeMillis();
			log.info("Finished copying " + recordCount + " records (" + byteCount + " bytes) for table " + sourceTable
					+ " in " + (endTime - startTime) + " ms");
			this.recordCount = recordCount;
		}
		catch (SQLException e)
		{
			log.severe("Error during data copy: " + e.getMessage());
			exception = new SQLException("Failed to copy table " + sourceTable + " with batch size " + batchSize, e);
		}
		log.fine(name + ": Finished copying");
	}

	public SQLException getException()
	{
		return exception;
	}

	public long getRecordCount()
	{
		return recordCount;
	}

	public long getByteCount()
	{
		return byteCount;
	}

}
