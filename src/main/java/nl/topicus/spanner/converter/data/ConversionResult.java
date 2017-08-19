package nl.topicus.spanner.converter.data;

public class ConversionResult
{
	private long recordCount;

	private long byteCount;

	ConversionResult()
	{
	}

	public void incRecordCount()
	{
		recordCount++;
	}

	public void incByteCount(long byteCount)
	{
		this.byteCount += byteCount;
	}

	protected long getRecordCount()
	{
		return recordCount;
	}

	protected void setRecordCount(long recordCount)
	{
		this.recordCount = recordCount;
	}

	protected long getByteCount()
	{
		return byteCount;
	}

	protected void setByteCount(long byteCount)
	{
		this.byteCount = byteCount;
	}

}
