package de.tuda.progressive.db.benchmark.adapter;

public interface JdbcAdapterFactory {
	JdbcAdapter create(String url);
}
