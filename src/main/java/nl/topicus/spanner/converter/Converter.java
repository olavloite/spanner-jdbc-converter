package nl.topicus.spanner.converter;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.data.DataConverter;
import nl.topicus.spanner.converter.ddl.IndexConverter;
import nl.topicus.spanner.converter.ddl.TableConverter;

public class Converter
{

	public static void main(String[] args)
	{
		if (args == null || args.length < 2 || args.length > 3)
			throw new IllegalArgumentException(
					"Unexpected number of arguments found. Usage: Converter urlSource urlDestination [configFile]");
		String urlSource = args[0];
		String urlDestination = args[1];
		ConverterConfiguration config;
		if (args.length > 2)
		{
			String configFile = args[2];
			try
			{
				config = new ConverterConfiguration(urlSource, urlDestination, Paths.get(configFile).toUri());
			}
			catch (IOException e)
			{
				throw new IllegalArgumentException(
						"Unable to find or read config file " + configFile + ": " + e.getMessage(), e);
			}
		}
		else
		{
			config = new ConverterConfiguration(urlSource, urlDestination);
		}

		if (config.getTableConvertMode() == ConvertMode.DropAndRecreate)
		{
			// Ask for confirmation.
			if (!confirm(
					"Conversion is running in DropAndRecreate mode for table creation. All existing tables in the destination database will be dropped. Do you want to continue (Y/N)? "))
				return;
		}
		if (config.getDataConvertMode() == ConvertMode.DropAndRecreate)
		{
			// Ask for confirmation.
			if (!confirm(
					"Conversion is running in DropAndRecreate mode for data upload. All existing data in the destination tables will be deleted. Do you want to continue (Y/N)? "))
				return;
		}
		try (Connection source = DriverManager.getConnection(urlSource);
				Connection destination = DriverManager.getConnection(urlDestination))
		{
			convert(source, destination, config);
		}
		catch (SQLException e)
		{
			System.err.println("Error converting database " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void convert(Connection source, Connection destination, ConverterConfiguration config)
			throws SQLException
	{
		if (config.getTableConvertMode() != ConvertMode.SkipAll)
		{
			TableConverter tableConverter = new TableConverter(source, destination, config);
			tableConverter.convert(null, null, true);

			IndexConverter indexConverter = new IndexConverter(source, destination, config);
			indexConverter.convert(null, null, true);
		}

		DataConverter dataConverter = new DataConverter(config);
		dataConverter.convert(null, null);
	}

	private static boolean confirm(String msg)
	{
		System.out.print(msg);
		if (System.console() == null)
		{
			System.out.println();
			System.out.println("No console detected. Please kill the application if you do not want to continue");
			for (int seconds = 5; seconds >= 0; seconds--)
			{
				System.out.println("Continue in " + seconds);
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException e)
				{
				}
			}
			return true;
		}
		else
		{
			String confirm = System.console().readLine();
			return "Y".equals(confirm);
		}
	}

}
