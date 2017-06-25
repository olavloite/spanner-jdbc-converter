package nl.topicus.spanner.converter.ddl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import nl.topicus.spanner.converter.ConvertMode;
import nl.topicus.spanner.converter.cfg.ConverterConfiguration;

public class IndexConverter
{
	private static final Logger log = Logger.getLogger(IndexConverter.class.getName());

	private final Connection source;

	private final Connection destination;

	private final Set<String> existingIndices = new HashSet<>();

	private final Set<String> primaryKeys = new HashSet<>();

	private final ConverterConfiguration config;

	public IndexConverter(Connection source, Connection destination, ConverterConfiguration config)
	{
		this.source = source;
		this.destination = destination;
		this.config = config;
	}

	private void initializeExistingIndices(String catalog, String schema) throws SQLException
	{
		existingIndices.clear();
		try (ResultSet tables = destination.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tables.next())
			{
				String table = tables.getString("TABLE_NAME");
				try (ResultSet indices = destination.getMetaData().getIndexInfo(catalog, schema, table, false, true))
				{
					while (indices.next())
					{
						existingIndices.add(indices.getString("INDEX_NAME").toUpperCase());
					}
				}
			}
		}
	}

	private void initializePrimaryKeys(String catalog, String schema) throws SQLException
	{
		primaryKeys.clear();
		try (ResultSet tables = source.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tables.next())
			{
				String table = tables.getString("TABLE_NAME");
				try (ResultSet pks = source.getMetaData().getPrimaryKeys(catalog, schema, table))
				{
					while (pks.next())
					{
						primaryKeys.add(pks.getString("PK_NAME").toUpperCase());
					}
				}
			}
		}
	}

	public String convert(String catalog, String schema, boolean create) throws SQLException
	{
		StringBuilder sql = new StringBuilder();
		initializeExistingIndices(catalog, schema);
		initializePrimaryKeys(catalog, schema);
		try (ResultSet tbls = source.getMetaData().getTables(catalog, schema, null, new String[] { "TABLE" }))
		{
			while (tbls.next())
			{
				String table = tbls.getString("TABLE_NAME");
				try (ResultSet indices = source.getMetaData().getIndexInfo(catalog, schema, table, false, true))
				{
					while (indices.next())
					{
						String indexName = indices.getString("INDEX_NAME");
						// Skip all primary keys
						if (!primaryKeys.contains(indexName.toUpperCase()))
						{
							boolean exists = existingIndices.contains(indices.getString("INDEX_NAME").toUpperCase());
							if (exists && config.getTableConvertMode() == ConvertMode.DropAndRecreate)
							{
								log.info("Index " + indices.getString("INDEX_NAME") + " already exists. Dropping index");
								dropIndex(indices.getString("INDEX_NAME"));
								log.info(indices.getString("INDEX_NAME") + " dropped");
							}
							String definition = getIndexDefinition(indices, exists);
							if (definition != null)
							{
								sql.append(definition).append("\n;\n\n");
								sql.append("/*---------------------------------------------------------------------*/\n");
								if (create)
								{
									log.info("Creating index " + indexName);
									destination.createStatement().executeUpdate(definition);
								}
								else
								{
									log.info("Index definition created: " + indexName);
								}
							}
							else
							{
								log.info("Skipping index " + indexName);
							}
						}
						while (!indices.isAfterLast() && indexName.equals(indices.getString("INDEX_NAME")))
						{
							indices.next();
						}
					}
				}
			}
		}
		return sql.toString();
	}

	private String getIndexDefinition(ResultSet indices, boolean exists) throws SQLException
	{
		String table = indices.getString("TABLE_NAME");
		String indexName = indices.getString("INDEX_NAME");
		ConvertMode createMode = config.getTableConvertMode();
		if (exists)
		{
			if (createMode == ConvertMode.SkipExisting)
				return null;
			if (createMode == ConvertMode.ThrowExceptionIfExists)
				throw new IllegalStateException("Index " + indexName + " already exists");
		}
		StringBuilder sql = new StringBuilder("CREATE INDEX ").append(indexName).append(" ON ").append(table)
				.append(" (\n");
		boolean first = true;
		while (true)
		{
			String currentName = indices.getString("INDEX_NAME");
			if (!indexName.equals(currentName))
				break;
			if (!first)
			{
				sql.append(",\n");
			}
			sql.append(indices.getString("COLUMN_NAME")).append(" ");
			String ascOrDesc = indices.getString("ASC_OR_DESC");
			if ("D".equals(ascOrDesc))
				sql.append("DESC ");
			first = false;
			if (!indices.next())
				break;
		}
		sql.append(")");
		return sql.toString();
	}

	private void dropIndex(String index) throws SQLException
	{
		String sql = "DROP INDEX " + index;
		destination.createStatement().executeUpdate(sql);
	}

}
