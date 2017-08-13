package nl.topicus.spanner.converter.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.util.ConverterUtils;

public class DataConverter
{
	private static final Logger log = Logger.getLogger(DataConverter.class.getName());

	private final ConverterConfiguration config;

	static final class Table
	{
		final String schema;

		final String name;

		Table(String schema, String name)
		{
			this.schema = schema;
			this.name = name;
		}

		@Override
		public String toString()
		{
			return schema + "." + name;
		}
	}

	static final class Columns
	{
		List<String> columnNames = new ArrayList<>();

		List<Integer> columnTypes = new ArrayList<>();

		List<String> primaryKeyCols = new ArrayList<>();

		String getColumnNames()
		{
			return String.join(", ", columnNames);
		}

		String getPrimaryKeyColumns()
		{
			return String.join(", ", primaryKeyCols);
		}

		String getColumnParameters()
		{
			String[] params = new String[columnNames.size()];
			Arrays.fill(params, "?");
			return String.join(", ", params);
		}
	}

	public DataConverter(ConverterConfiguration config)
	{
		this.config = config;
	}

	public void convert(String catalog, String schema) throws SQLException
	{
		try (Connection source = DriverManager.getConnection(config.getUrlSource());
				Connection destination = DriverManager.getConnection(config.getUrlDestination()))
		{
			destination.setAutoCommit(false);
			source.setAutoCommit(false);
			source.setReadOnly(true);
			List<Table> tablesList = new ArrayList<>();
			try (ResultSet tables = destination.getMetaData().getTables(catalog, schema, null,
					new String[] { "TABLE" }))
			{
				while (tables.next())
				{
					String tableSchema = tables.getString("TABLE_SCHEM");
					if (!config.getDestinationDatabaseType().isSystemSchema(tableSchema))
					{
						tablesList.add(new Table(tableSchema, tables.getString("TABLE_NAME")));
					}
				}
			}
			for (Table table : tablesList)
			{
				long destinationRecordCount = getDestinationRecordCount(destination, table.name);
				if (destinationRecordCount > 0 && config.getDataConvertMode() == ConvertMode.DropAndRecreate)
				{
					deleteAll(destination, catalog, schema, table.name, destinationRecordCount);
				}
			}
			int tablesPerWorker = Math.max(Math.min(tablesList.size() / config.getNumberOfTableWorkers(), 8), 1);
			List<List<Table>> partionedTables = ConverterUtils.partition(tablesList, tablesPerWorker);
			convertListOfTables(catalog, partionedTables);

			source.commit();
		}
	}

	private void convertListOfTables(String catalog, List<List<Table>> partionedTables)
	{
		ExecutorService service = Executors.newFixedThreadPool(config.getNumberOfTableWorkers());
		for (List<Table> tables : partionedTables)
		{
			TableListWorker worker = new TableListWorker(config, catalog, tables);
			service.submit(worker);
		}
		service.shutdown();
		try
		{
			service.awaitTermination(config.getUploadWorkerMaxWaitInMinutes(), TimeUnit.MINUTES);
		}
		catch (InterruptedException e)
		{
			log.severe("Error while waiting for workers to finish: " + e.getMessage());
			throw new RuntimeException(e);
		}
	}

	static int getSourceRecordCount(Connection source, String tableSpec) throws SQLException
	{
		try (ResultSet count = source.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableSpec))
		{
			if (count.next())
				return count.getInt(1);
		}
		return 0;
	}

	static long getDestinationRecordCount(Connection destination, String table) throws SQLException
	{
		String sql = "select count(*) from " + table;
		try (ResultSet rs = destination.createStatement().executeQuery(sql))
		{
			if (rs.next())
				return rs.getLong(1);
		}
		finally
		{
			destination.commit();
		}
		return 0;
	}

	private void deleteAll(Connection destination, String catalog, String schema, String table, long recordCount)
			throws SQLException
	{
		log.info("Deleting all existing records from " + table);
		if (recordCount >= 15000)
		{
			log.info("Deleting in batches as record count is " + recordCount);
			Columns columns = TableListWorker.getColumns(destination, catalog, schema, table, true);
			boolean first = true;
			String sql = "DELETE FROM " + table + " WHERE ";
			for (String col : columns.primaryKeyCols)
			{
				if (!first)
					sql = sql + " AND ";
				sql = sql + col + "=?";
				first = false;
			}
			PreparedStatement ps = destination.prepareStatement(sql);

			try (ResultSet rs = destination
					.prepareStatement("SELECT " + columns.getPrimaryKeyColumns() + " FROM " + table).executeQuery())
			{
				int counter = 0;
				while (rs.next())
				{
					int i = 1;
					for (String col : columns.primaryKeyCols)
					{
						ps.setObject(i, rs.getObject(col));
						i++;
					}
					ps.executeUpdate();
					counter++;
					if (counter % 1000 == 0)
					{
						destination.commit();
						log.info("Records deleted so far: " + counter);
					}
				}
				destination.commit();
				log.info("Finished deleting " + counter + " records");
			}
		}
		else
		{
			String sql = "delete from " + table;
			destination.createStatement().executeUpdate(sql);
			destination.commit();
		}
		log.info("Delete done on " + table);
	}

}
