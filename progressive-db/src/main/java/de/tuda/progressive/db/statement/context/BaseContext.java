package de.tuda.progressive.db.statement.context;

import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Optional;

public abstract class BaseContext {

	private final SqlSelect selectSource;

	private final List<MetaField> metaFields;

	public BaseContext(
			SqlSelect selectSource,
			List<MetaField> metaFields
	) {
		this.selectSource = selectSource;
		this.metaFields = metaFields;
	}

	public SqlSelect getSelectSource() {
		return selectSource;
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

	@SuppressWarnings("unchecked")
	public abstract static class Builder<C extends BaseContext, B extends Builder> {
		private SqlSelect selectSource;

		private List<MetaField> metaFields;

		public B selectSource(SqlSelect selectSource) {
			this.selectSource = selectSource;
			return (B) this;
		}

		public B metaFields(List<MetaField> metaFields) {
			this.metaFields = metaFields;
			return (B) this;
		}

		public final C build() {
			return (C) build(selectSource, metaFields);
		}

		protected abstract BaseContext build(SqlSelect selectSource, List<MetaField> metaFields);
	}
}
