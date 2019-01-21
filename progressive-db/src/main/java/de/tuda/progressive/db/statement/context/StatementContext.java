package de.tuda.progressive.db.statement.context;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StatementContext {

	private final SqlSelect selectSource;

	private final SqlCreateTable createCache;

	private final SqlInsert insertCache;

	private final SqlSelect selectCache;

	private final List<Aggregation> aggregations;

	private final Map<MetaField, Integer> metaFieldPositions;

	public StatementContext(
			SqlSelect selectSource,
			SqlCreateTable createCache,
			SqlInsert insertCache,
			SqlSelect selectCache,
			List<Aggregation> aggregations,
			Map<MetaField, Integer> metaFieldPositions
	) {
		this.selectSource = selectSource;
		this.createCache = createCache;
		this.insertCache = insertCache;
		this.selectCache = selectCache;
		this.aggregations = aggregations;
		this.metaFieldPositions = metaFieldPositions;
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

	public Optional<Integer> getMetaFieldPosition(MetaField metaField) {
		return Optional.of(metaFieldPositions.get(metaField));
	}
}
