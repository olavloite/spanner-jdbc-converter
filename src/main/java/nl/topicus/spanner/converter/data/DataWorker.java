package nl.topicus.spanner.converter.data;

import java.util.concurrent.Callable;

import nl.topicus.spanner.converter.cfg.ConverterConfiguration;
import nl.topicus.spanner.converter.util.ConverterUtils;

public abstract class DataWorker implements Callable<ConversionResult>
{
	protected final String name;

	protected final ConverterConfiguration config;

	protected final ConverterUtils converterUtils;

	protected ConversionResult result = new ConversionResult();

	DataWorker(String name, ConverterConfiguration config)
	{
		this.name = name;
		this.config = config;
		this.converterUtils = new ConverterUtils(config);
	}

}
