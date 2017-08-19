package nl.topicus.spanner.converter.data;

final class Table
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