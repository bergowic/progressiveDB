package de.tuda.progressive.db.benchmark.adapter;

import de.tuda.progressive.db.benchmark.adapter.impl.MonetDBAdapter;
import de.tuda.progressive.db.benchmark.adapter.impl.PostgresAdapter;

public class SimpleJdbcAdapterFactory implements JdbcAdapterFactory {

	private static final int PREFIX_LEN = "jdbc:".length();

	@Override
	public JdbcAdapter create(String url) {
		final String driver = url.substring(PREFIX_LEN, url.indexOf(":", PREFIX_LEN));
		switch (driver.toUpperCase()) {
			case "POSTGRESQL":
				return new PostgresAdapter(url);
			case "MONETDB":
				return new MonetDBAdapter(url);
		}

		throw new IllegalArgumentException("driver not supported: " + driver);
	}
}
