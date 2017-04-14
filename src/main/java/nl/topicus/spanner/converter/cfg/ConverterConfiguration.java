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
	private final Properties properties = new Properties();

	private ConvertMode tableConvertMode;

	private ConvertMode dataConvertMode;

	private final String urlSource;

	private final String urlDestination;

	/**
	 * Create a default converter configuration
	 */
	public ConverterConfiguration(String urlSource, String urlDestination)
	{
		this.urlSource = urlSource;
		this.urlDestination = urlDestination;
		tableConvertMode = ConvertMode.SkipExisting;
		dataConvertMode = ConvertMode.SkipExisting;
	}

	/**
	 * Create a converter configuration from a properties file
	 */
	public ConverterConfiguration(String urlSource, String urlDestination, URI file) throws IOException
	{
		this.urlSource = urlSource;
		this.urlDestination = urlDestination;
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

	public String getUrlSource()
	{
		return urlSource;
	}

	public String getUrlDestination()
	{
		return urlDestination;
	}

}
