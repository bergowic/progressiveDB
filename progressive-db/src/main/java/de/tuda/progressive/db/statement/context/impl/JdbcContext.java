package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.BaseContext;
import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

public abstract class JdbcContext extends BaseContext {

	private final SqlCreateTable createBuffer;

	private final SqlSelect selectBuffer;

	public JdbcContext(
			SqlSelect selectSource,
			List<MetaField> metaFields,
			SqlCreateTable createBuffer,
			SqlSelect selectBuffer
	) {
		super(selectSource, metaFields);
		this.createBuffer = createBuffer;
		this.selectBuffer = selectBuffer;
	}

	public SqlCreateTable getCreateBuffer() {
		return createBuffer;
	}

	public SqlSelect getSelectBuffer() {
		return selectBuffer;
	}

	@SuppressWarnings("unchecked")
	public static abstract class Builder<C extends JdbcContext, B extends Builder> extends BaseContext.Builder<C, B> {
		private SqlCreateTable createBuffer;

		private SqlSelect selectBuffer;

		public B createBuffer(SqlCreateTable createBuffer) {
			this.createBuffer = createBuffer;
			return (B) this;
		}

		public B selectBuffer(SqlSelect selectBuffer) {
			this.selectBuffer = selectBuffer;
			return (B) this;
		}

		@Override
		protected final C build(SqlSelect selectSource, List<MetaField> metaFields) {
			return (C) build(selectSource, metaFields, createBuffer, selectBuffer);
		}

		protected abstract C build(
				SqlSelect selectSource,
				List<MetaField> metaFields,
				SqlCreateTable createBuffer,
				SqlSelect selectBuffer
		);
	}
}
