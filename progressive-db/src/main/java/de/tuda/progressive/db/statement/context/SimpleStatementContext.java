package de.tuda.progressive.db.statement.context;

import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SimpleStatementContext {

	private final SqlSelect selectCache;

	private final List<Aggregation> aggregations;

	private final Map<MetaField, Integer> metaFieldPositions;

	public SimpleStatementContext(
			SqlSelect selectCache,
			List<Aggregation> aggregations,
			Map<MetaField, Integer> metaFieldPositions
	) {
		this.selectCache = selectCache;
		this.aggregations = aggregations;
		this.metaFieldPositions = metaFieldPositions;
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

	public Map<MetaField, Integer> getMetaFieldPositions() {
		return metaFieldPositions;
	}
}
