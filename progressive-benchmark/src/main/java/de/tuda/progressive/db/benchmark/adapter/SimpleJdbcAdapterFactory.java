package de.tuda.progressive.db.benchmark.adapter;

import de.tuda.progressive.db.benchmark.adapter.impl.MonetDBAdapter;
import de.tuda.progressive.db.benchmark.adapter.impl.PostgresHashPartitionAdapter;
import de.tuda.progressive.db.benchmark.adapter.impl.PostgresRangePartitionAdapter;

public class SimpleJdbcAdapterFactory implements JdbcAdapterFactory {

	private static final int PREFIX_LEN = "jdbc:".length();

	@Override
	public JdbcAdapter create(String url, String type) {
		final String driver = url.substring(PREFIX_LEN, url.indexOf(":", PREFIX_LEN));
		switch (driver.toUpperCase()) {
			case "POSTGRESQL":
				if (type == null) {
					return new PostgresRangePartitionAdapter(url);
				}
				switch (type.toUpperCase()) {
					case "RANGE":
						return new PostgresRangePartitionAdapter(url);
					case "HASH":
						return new PostgresHashPartitionAdapter(url);
				}
			case "MONETDB":
				return new MonetDBAdapter(url);
		}

		throw new IllegalArgumentException("driver not supported: " + driver);
	}
}
