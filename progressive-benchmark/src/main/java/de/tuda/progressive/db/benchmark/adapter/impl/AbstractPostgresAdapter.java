package de.tuda.progressive.db.benchmark.adapter.impl;

import de.tuda.progressive.db.benchmark.adapter.AbstractJdbcAdapter;

public abstract class AbstractPostgresAdapter extends AbstractJdbcAdapter {

	private static final String COPY_TPL = "copy %s from '%s' delimiter '|' null ''";
	private static final String ANALYZE_TPL = "analyze %s";

	public AbstractPostgresAdapter(String url) {
		super(url);
	}

	@Override
	protected String getCopyTemplate() {
		return COPY_TPL;
	}

	@Override
	public void analyze(String table) {
		execute(ANALYZE_TPL, table);
	}

	@Override
	protected String escapePath(String path) {
		return path;
	}

	@Override
	public String getDriverName() {
		return "postgresql";
	}

	protected final String getTmpTable(String table) {
		return String.format("%s_tmp", table);
	}
}
