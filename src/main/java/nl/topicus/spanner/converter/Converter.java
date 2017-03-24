package nl.topicus.spanner.converter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import nl.topicus.spanner.converter.data.DataConverter;
import nl.topicus.spanner.converter.ddl.TableConverter;

public class Converter
{

	public static void main(String[] args)
	{
		if (args == null || args.length != 2)
			throw new IllegalArgumentException(
					"Unexpected number of arguments found. Usage: Converter urlSource urlDestination");
		String urlSource = args[0];
		String urlDestination = args[1];
		try (Connection source = DriverManager.getConnection(urlSource);
				Connection destination = DriverManager.getConnection(urlDestination))
		{
			convert(source, destination);
		}
		catch (SQLException e)
		{
			System.err.println("Error converting database " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void convert(Connection source, Connection destination) throws SQLException
	{
		TableConverter tableConverter = new TableConverter(source, destination, ConvertMode.SkipExisting);
		configureTableConverter(tableConverter);
		tableConverter.convert("", "", true);

		DataConverter dataConverter = new DataConverter(source, destination, ConvertMode.SkipExisting);
		dataConverter.convert("", "");
	}

	private static void configureTableConverter(TableConverter tableConverter)
	{
		tableConverter.registerSpecificColumnMapping("uuid", "BYTES(16)");
	}

}
