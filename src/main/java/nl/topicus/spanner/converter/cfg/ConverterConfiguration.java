package nl.topicus.spanner.converter.cfg;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import nl.topicus.spanner.converter.ConvertMode;

public class ConverterConfiguration
{
	private Properties properties = new Properties();

	private ConvertMode tableConvertMode;

	private ConvertMode dataConvertMode;

	/**
	 * Create a default converter configuration
	 */
	public ConverterConfiguration()
	{
		tableConvertMode = ConvertMode.SkipExisting;
		dataConvertMode = ConvertMode.SkipExisting;
	}

	/**
	 * Create a converter configuration from a properties file
	 */
	public ConverterConfiguration(URI file) throws IOException
	{
		properties.load(Files.newBufferedReader(Paths.get(file)));
	}

	public ConvertMode getTableConvertMode()
	{
		if (tableConvertMode == null)
		{
			tableConvertMode = ConvertMode.valueOf(ConvertMode.class,
					properties.getProperty("TableConverter.convertMode"));
		}
		return tableConvertMode;
	}

	public ConvertMode getDataConvertMode()
	{
		if (dataConvertMode == null)
		{
			dataConvertMode = ConvertMode.valueOf(ConvertMode.class,
					properties.getProperty("DataConverter.convertMode"));
		}
		return dataConvertMode;
	}

	public Map<String, String> getSpecificColumnMappings()
	{
		Map<String, String> res = new HashMap<String, String>();
		for (String key : properties.stringPropertyNames())
		{
			if (key.startsWith("TableConverter.specificColumnMapping."))
			{
				String column = key.substring("TableConverter.specificColumnMapping.".length());
				String value = properties.getProperty(key);
				res.put(column, value);
			}
		}

		return res;
	}

}
