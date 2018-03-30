package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.util.ConverterUtils;

public abstract class AbstractTablePartWorker implements Callable<ConversionResult>
{
	private static final Logger log = Logger.getLogger(AbstractTablePartWorker.class.getName());

	protected final ConverterConfiguration config;

	protected final ConverterUtils converterUtils;

	protected final String sourceTable;

	private final String destinationTable;

	protected final Columns insertCols;

	private long byteCount;

	private long actualRecordCount;

	private Exception exception;

	AbstractTablePartWorker(ConverterConfiguration config, String sourceTable)
	{
		this(config, sourceTable, null, null);
	}

	AbstractTablePartWorker(ConverterConfiguration config, String sourceTable, String destinationTable,
			Columns insertCols)
	{
		this.config = config;
		this.sourceTable = sourceTable;
		this.destinationTable = destinationTable;
		this.insertCols = insertCols;
		this.converterUtils = new ConverterUtils(config);
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
		return new ConversionResult(getRecordCount(), getByteCount(), startTime, endTime, exception);
	}

	protected PreparedStatement createInsertStatement(Connection destination) throws SQLException
	{
		String sql = "INSERT INTO " + destinationTable + " (" + insertCols.getColumnNames() + ") VALUES \n";
		sql = sql + "(" + insertCols.getColumnParameters() + ")";
		return destination.prepareStatement(sql);
	}

	protected void copyResultSet(ResultSet rs, PreparedStatement insertStatement) throws SQLException
	{
		while (rs.next())
		{
			int index = 1;
			for (Integer type : insertCols.getColumnTypes())
			{
				Object object = rs.getObject(index);
				insertStatement.setObject(index, object, type);
				byteCount += converterUtils.getActualDataSize(type, object);
				index++;
			}
			if (config.isUseJdbcBatching())
			{
				insertStatement.addBatch();
			}
			else
			{
				insertStatement.executeUpdate();
			}
			actualRecordCount++;
			if (config.isUseJdbcBatching() && actualRecordCount % config.getMaxStatementsInOneJdbcBatch() == 0)
			{
				insertStatement.executeBatch();
				log.info(toString() + " - Current record count for " + sourceTable + ": " + actualRecordCount);
			}
		}
		if (config.isUseJdbcBatching())
		{
			insertStatement.executeBatch();
		}
	}

	protected abstract void run() throws Exception;

	protected long getByteCount()
	{
		return byteCount;
	}

	protected long getRecordCount()
	{
		return actualRecordCount;
	}

	Exception getException()
	{
		return exception;
	}

	void setException(Exception exception)
	{
		this.exception = exception;
	}

}
