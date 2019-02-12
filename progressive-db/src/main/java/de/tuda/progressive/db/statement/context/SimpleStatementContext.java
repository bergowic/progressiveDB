package de.tuda.progressive.db.statement.context;

import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Optional;

public class SimpleStatementContext {

	private final SqlSelect selectCache;

	private final List<MetaField> metaFields;

	public SimpleStatementContext(
			SqlSelect selectCache,
			List<MetaField> metaFields
	) {
		this.selectCache = selectCache;
		this.metaFields = metaFields;
	}

	public SqlSelect getSelectCache() {
		return selectCache;
	}

	public Optional<Integer> getFunctionMetaFieldPos(MetaField metaField, boolean substitute) {
		if (!metaField.isFunction()) {
			throw new IllegalArgumentException("metaField must be a function");
		}

		int pos = 0;
		for (MetaField m : metaFields) {
			if (m == metaField) {
				return Optional.of(pos);
			}

			if (!substitute || m.isSubstitute()) {
				pos++;
			}
		}

		return Optional.empty();
	}

	public List<MetaField> getMetaFields() {
		return metaFields;
	}
}
