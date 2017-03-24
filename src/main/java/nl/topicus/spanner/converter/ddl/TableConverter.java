package nl.topicus.spanner.converter.ddl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class TableConverter
{
	private static final Logger log = Logger.getLogger(TableConverter.class.getName());

	private final Map<Integer, String> columnTypes = new HashMap<>();

	private final Connection source;

	private final Connection destination;

	private final Set<String> existingTables = new HashSet<>();

	private final Map<String, String> specificColumnMapping = new HashMap<>();

	private final ConverterConfiguration config;

	private int defaultSizeBytes = 1000000;

	private int defaultSizeString = 4000;

	private int defaultSizeOther = 100;

	private int maxSizeColumn = 1000000;

	public TableConverter(Connection source, Connection destination, ConverterConfiguration config)
	{
		this.source = source;
		this.destination = destination;
		this.config = config;
		registerDefaultColumnTypes();
		registerConfiguredColumnMappings();
	}

	private void registerDefaultColumnTypes()
	{
		registerColumnType(Types.BOOLEAN, "BOOL");
		registerColumnType(Types.BIT, "BOOL");
		registerColumnType(Types.BIGINT, "INT64");
		registerColumnType(Types.SMALLINT, "INT64");
		registerColumnType(Types.TINYINT, "INT64");
		registerColumnType(Types.INTEGER, "INT64");
		registerColumnType(Types.CHAR, "STRING(1)");
		registerColumnType(Types.VARCHAR, "STRING($1)");
		registerColumnType(Types.FLOAT, "FLOAT64");
		registerColumnType(Types.DOUBLE, "FLOAT64");
		registerColumnType(Types.DECIMAL, "FLOAT64");
		registerColumnType(Types.DATE, "DATE");
		registerColumnType(Types.TIME, "TIMESTAMP");
		registerColumnType(Types.TIMESTAMP, "TIMESTAMP");
		registerColumnType(Types.VARBINARY, "BYTES($1)");
		registerColumnType(Types.BINARY, "BYTES($1)");
		registerColumnType(Types.LONGVARCHAR, "STRING($1)");
		registerColumnType(Types.LONGVARBINARY, "BYTES($1)");
		registerColumnType(Types.CLOB, "STRING($1)");
		registerColumnType(Types.BLOB, "BYTES($1)");
		registerColumnType(Types.NUMERIC, "FLOAT64");
	}

	private void registerConfiguredColumnMappings()
	{
		Map<String, String> mappings = config.getSpecificColumnMappings();
		for (String col : mappings.keySet())
		{
			String dataType = mappings.get(col);
			registerSpecificColumnMapping(col, dataType);
		}
	}

	public void registerColumnType(Integer sqlType, String cloudSpannerType)
	{
		columnTypes.put(sqlType, cloudSpannerType);
	}

	public void registerSpecificColumnMapping(String column, String dataType)
	{
		specificColumnMapping.put(column, dataType);
	}

	private void initializeExistingTables(String catalog, String schema) throws SQLException
	{
		existingTables.clear();
		try (ResultSet tables = destination.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tables.next())
			{
				existingTables.add(tables.getString("TABLE_NAME").toUpperCase());
			}
		}
	}

	public String convert(String catalog, String schema, boolean create) throws SQLException
	{
		StringBuilder sql = new StringBuilder();
		initializeExistingTables(catalog, schema);
		try (ResultSet tables = source.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tables.next())
			{
				boolean exists = existingTables.contains(tables.getString("TABLE_NAME").toUpperCase());
				if (exists && config.getTableConvertMode() == ConvertMode.DropAndRecreate)
				{
					log.info("Table " + tables.getString("TABLE_NAME") + " already exists. Dropping table");
					dropTable(tables.getString("TABLE_NAME"));
					log.info(tables.getString("TABLE_NAME") + " dropped");
				}
				String definition = getTableDefinition(tables, exists);
				if (definition != null)
				{
					sql.append(definition).append("\n;\n\n");
					sql.append("/*---------------------------------------------------------------------*/\n");
					if (create)
					{
						log.info("Creating table " + tables.getString("TABLE_NAME"));
						destination.createStatement().executeUpdate(definition);
					}
					else
					{
						log.info("Table definition created: " + tables.getString("TABLE_NAME"));
					}
				}
				else
				{
					log.info("Skipping table " + tables.getString("TABLE_NAME"));
				}
			}
		}
		return sql.toString();
	}

	private String getTableDefinition(ResultSet tables, boolean exists) throws SQLException
	{
		String catalog = tables.getString("TABLE_CAT");
		String schema = tables.getString("TABLE_SCHEM");
		String table = tables.getString("TABLE_NAME");
		ConvertMode createMode = config.getTableConvertMode();
		if (exists)
		{
			if (createMode == ConvertMode.SkipExisting)
				return null;
			if (createMode == ConvertMode.ThrowExceptionIfExists)
				throw new IllegalStateException("Table " + table + " already exists");
		}
		StringBuilder sql = new StringBuilder("CREATE TABLE ").append(table).append(" (\n");
		try (ResultSet columns = source.getMetaData().getColumns(catalog, schema, table, null))
		{
			boolean first = true;
			while (columns.next())
			{
				if (!first)
				{
					sql.append(",\n");
				}
				sql.append(columns.getString("COLUMN_NAME")).append(" ");
				sql.append(getColumnDataType(columns)).append(" ");
				sql.append(getNotNull(columns));
				first = false;
			}
			sql.append(")");
		}
		try (ResultSet keys = source.getMetaData().getPrimaryKeys(catalog, schema, table))
		{
			sql.append(" PRIMARY KEY (");
			boolean first = true;
			while (keys.next())
			{
				if (!first)
				{
					sql.append(", ");
				}
				sql.append(keys.getString("COLUMN_NAME"));
				first = false;
			}
			sql.append(")");
		}
		return sql.toString();
	}

	private String getColumnDataType(ResultSet columns) throws SQLException
	{
		String columnName = columns.getString("COLUMN_NAME");
		String tableName = columns.getString("TABLE_NAME");
		String specificMapping = specificColumnMapping.get(tableName + "." + columnName);
		if (specificMapping == null)
			specificMapping = specificColumnMapping.get(columnName);
		if (specificMapping != null)
			return specificMapping;

		int type = columns.getInt("DATA_TYPE");
		int size = columns.getInt("COLUMN_SIZE");
		String cloudSpannerType = columnTypes.get(Integer.valueOf(type));
		if (cloudSpannerType == null)
			throw new IllegalArgumentException("No mapping found for SQL type " + type);
		if (cloudSpannerType.contains("$1"))
		{
			int useSize = Math.min(size, maxSizeColumn);
			if (useSize == 0)
				useSize = getDefaultSize(cloudSpannerType);
			cloudSpannerType = cloudSpannerType.replace("$1", String.valueOf(useSize));
		}
		return cloudSpannerType;
	}

	private String getNotNull(ResultSet columns) throws SQLException
	{
		int nullable = columns.getInt("NULLABLE");
		if (nullable == DatabaseMetaData.columnNoNulls)
			return "NOT NULL";
		return "";
	}

	private int getDefaultSize(String cloudSpannerType)
	{
		if ("STRING".equals(cloudSpannerType))
			return defaultSizeString;
		if ("BYTES".equals(cloudSpannerType))
			return defaultSizeBytes;

		return defaultSizeOther;
	}

	private void dropTable(String table) throws SQLException
	{
		String sql = "DROP TABLE " + table;
		destination.createStatement().executeUpdate(sql);
	}

}
