package de.tuda.progressive.db.benchmark.adapter;

public interface JdbcAdapterFactory {
	default JdbcAdapter create(String url) {
		return create(url, null);
	}

	JdbcAdapter create(String url, String type);
}
