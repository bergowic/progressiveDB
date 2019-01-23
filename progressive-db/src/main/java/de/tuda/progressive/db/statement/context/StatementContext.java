package de.tuda.progressive.db.statement.context;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;
import java.util.Map;

public class StatementContext extends SimpleStatementContext {

	private final SqlSelect selectSource;

	private final SqlCreateTable createCache;

	private final SqlInsert insertCache;

	public StatementContext(
			SqlSelect selectSource,
			SqlCreateTable createCache,
			SqlInsert insertCache,
			SqlSelect selectCache,
			List<Aggregation> aggregations,
			Map<MetaField, Integer> metaFieldPositions
	) {
		super(selectCache, aggregations, metaFieldPositions);

		this.selectSource = selectSource;
		this.createCache = createCache;
		this.insertCache = insertCache;
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
}
