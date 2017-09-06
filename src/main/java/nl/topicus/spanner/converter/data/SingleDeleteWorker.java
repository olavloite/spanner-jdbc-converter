package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class SingleDeleteWorker extends AbstractTablePartWorker
{

	SingleDeleteWorker(ConverterConfiguration config, String table, long recordCount)
	{
		super(config, table, recordCount);
	}

	@Override
	public void run() throws Exception
	{
		try (Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			String sql = "delete from " + table;
			destination.createStatement().executeUpdate(sql);
			destination.commit();
		}
	}

	@Override
	protected long getByteCount()
	{
		return 0;
	}

}
