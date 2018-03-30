package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class SingleDeleteWorker extends AbstractTablePartWorker
{
	private final long recordCount;

	SingleDeleteWorker(ConverterConfiguration config, String table, long recordCount)
	{
		super(config, table);
		this.recordCount = recordCount;
	}

	@Override
	public void run() throws Exception
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			String sql = "delete from " + sourceTable;
			destination.createStatement().executeUpdate(sql);
			destination.commit();
		}
	}

	@Override
	protected long getByteCount()
	{
		return 0;
	}

	@Override
	protected long getRecordCount()
	{
		return recordCount;
	}

}
