package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.util.ConverterUtils;

public class UploadWorker extends AbstractTablePartWorker
{
	private static final Logger log = Logger.getLogger(UploadWorker.class.getName());

	private String selectFormat;

	private String sourceTable;

	private String destinationTable;

	private Columns insertCols;

	private Columns selectCols;

	private long beginOffset;

	private int batchSize;

	private long byteCount;

	UploadWorker(String name, ConverterConfiguration config, String selectFormat, String sourceTable,
			String destinationTable, Columns insertCols, Columns selectCols, long beginOffset,
			long numberOfRecordsToCopy, int batchSize)
	{
		super(config, sourceTable, numberOfRecordsToCopy);
		this.selectFormat = selectFormat;
		this.sourceTable = sourceTable;
		this.destinationTable = destinationTable;
		this.insertCols = insertCols;
		this.selectCols = selectCols;
		this.beginOffset = beginOffset;
		this.batchSize = batchSize;
	}

	@Override
	public void run() throws SQLException
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination());
				Connection source = DriverManager.getConnection(config.getUrlSource()))
		{
			ConverterUtils converterUtils = new ConverterUtils(config);
			log.fine(sourceTable + ": Starting copying " + totalRecordCount + " records");

			destination.setAutoCommit(false);
			String sql = "INSERT INTO " + destinationTable + " (" + insertCols.getColumnNames() + ") VALUES \n";
			sql = sql + "(" + insertCols.getColumnParameters() + ")";
			PreparedStatement statement = destination.prepareStatement(sql);

			long lastRecord = beginOffset + totalRecordCount;
			long recordCount = 0;
			long currentOffset = beginOffset;
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
					while (rs.next())
					{
						int index = 1;
						for (Integer type : insertCols.getColumnTypes())
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object, type);
							byteCount += converterUtils.getActualDataSize(type, object);
							index++;
						}
						if (config.isUseJdbcBatching())
							statement.addBatch();
						else
							statement.executeUpdate();
						recordCount++;
					}
					if (config.isUseJdbcBatching())
						statement.executeBatch();
				}
				destination.commit();
				log.fine(sourceTable + ": Records copied so far: " + recordCount + " of " + totalRecordCount);
				currentOffset = currentOffset + batchSize;
				if (recordCount >= totalRecordCount)
					break;
			}
		}
		log.fine(sourceTable + ": Finished copying");
	}

	@Override
	protected long getByteCount()
	{
		return byteCount;
	}

}
