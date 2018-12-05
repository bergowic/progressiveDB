package de.tuda.progressive.db.statement;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

public class StatementContext {

	private final SqlSelect selectSource;

	private final SqlCreateTable createCache;

	private final SqlInsert insertCache;

	private final SqlSelect selectCache;

	private final List<Aggregation> aggregations;

	public StatementContext(SqlSelect selectSource, SqlCreateTable createCache, SqlInsert insertCache, SqlSelect selectCache, List<Aggregation> aggregations) {
		this.selectSource = selectSource;
		this.createCache = createCache;
		this.insertCache = insertCache;
		this.selectCache = selectCache;
		this.aggregations = aggregations;

	}

	public SqlSelect getSelectSource() {
		return selectSource;
	}

	public SqlCreateTable getCreateCache() {
		return createCache;
	}

	public SqlInsert getInsertCache() {
		return insertCache;
	}

	public SqlSelect getSelectCache() {
		return selectCache;
	}

	public List<Aggregation> getAggregations() {
		return aggregations;
	}
}
