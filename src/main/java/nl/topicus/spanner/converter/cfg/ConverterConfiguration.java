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
	public static enum DatabaseType
	{
		CloudSpanner
		{
			@Override
			public boolean isType(String url)
			{
				return url.toLowerCase().startsWith("jdbc:cloudspanner");
			}
		},
		PostgreSQL
		{
			@Override
			public boolean isType(String url)
			{
				return url.toLowerCase().startsWith("jdbc:postgresql");
			}
		};

		public abstract boolean isType(String url);

		public static DatabaseType getType(String url)
		{
			for (DatabaseType type : DatabaseType.values())
				if (type.isType(url))
					return type;
			return null;
		}
	}

	private final Properties properties = new Properties();

	private ConvertMode tableConvertMode;

	private ConvertMode dataConvertMode;

	private Integer batchSize;

	private Integer maxNumberOfWorkers;

	/**
	 * Maximum time to wait for a worker to finish in minutes
	 */
	private Integer uploadWorkerMaxWaitInMinutes;

	private Boolean useJdbcBatching;

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
					properties.getProperty("TableConverter.convertMode", ConvertMode.SkipExisting.name()));
		}
		return tableConvertMode;
	}

	public ConvertMode getDataConvertMode()
	{
		if (dataConvertMode == null)
		{
			dataConvertMode = ConvertMode.valueOf(ConvertMode.class,
					properties.getProperty("DataConverter.convertMode", ConvertMode.SkipExisting.name()));
		}
		return dataConvertMode;
	}

	public Integer getBatchSize()
	{
		if (batchSize == null)
		{
			batchSize = Integer.valueOf(properties.getProperty("DataConverter.batchSize", "1000"));
		}
		return batchSize;
	}

	public Integer getMaxNumberOfWorkers()
	{
		if (maxNumberOfWorkers == null)
		{
			maxNumberOfWorkers = Integer.valueOf(properties.getProperty("DataConverter.maxNumberOfWorkers", "10"));
		}
		return maxNumberOfWorkers;
	}

	public Integer getUploadWorkerMaxWaitInMinutes()
	{
		if (uploadWorkerMaxWaitInMinutes == null)
		{
			uploadWorkerMaxWaitInMinutes = Integer.valueOf(properties.getProperty(
					"DataConverter.uploadWorkerMaxWaitInMinutes", "60"));
		}
		return uploadWorkerMaxWaitInMinutes;
	}

	public boolean isUseJdbcBatching()
	{
		if (useJdbcBatching == null)
		{
			useJdbcBatching = Boolean.valueOf(properties.getProperty("DataConverter.useJdbcBatching", "true"));
		}
		return useJdbcBatching.booleanValue();
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

	public DatabaseType getSourceDatabaseType()
	{
		return DatabaseType.getType(urlSource);
	}

	public DatabaseType getDestinationDatabaseType()
	{
		return DatabaseType.getType(urlDestination);
	}

}
