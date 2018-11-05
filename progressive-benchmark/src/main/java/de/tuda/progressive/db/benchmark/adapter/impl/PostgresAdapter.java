package de.tuda.progressive.db.benchmark.adapter.impl;

import de.tuda.progressive.db.benchmark.adapter.AbstractJdbcAdapter;

import java.sql.SQLException;

public class PostgresAdapter extends AbstractJdbcAdapter {

	private static final String COPY_TEMPLATE = "COPY %s FROM '%s' DELIMITER '|' NULL ''";

	public PostgresAdapter(String url) throws SQLException {
		super(url);
	}

	@Override
	protected String getCopyTemplate() {
		return COPY_TEMPLATE;
	}
}
