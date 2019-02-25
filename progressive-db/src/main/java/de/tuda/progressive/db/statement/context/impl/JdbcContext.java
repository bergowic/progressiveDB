package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.BaseContext;
import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

public class JdbcContext extends BaseContext {

	private final SqlCreateTable createBuffer;

	private final SqlInsert insertBuffer;

	private final SqlUpdate updateBuffer;

	private final SqlSelect selectBuffer;


	public JdbcContext(
			SqlSelect selectSource,
			List<MetaField> metaFields,
			SqlCreateTable createBuffer,
			SqlInsert insertBuffer,
			SqlUpdate updateBuffer,
			SqlSelect selectBuffer
	) {
		super(selectSource, metaFields);

		this.createBuffer = createBuffer;
		this.insertBuffer = insertBuffer;
		this.updateBuffer = updateBuffer;
		this.selectBuffer = selectBuffer;
	}

	public SqlCreateTable getCreateBuffer() {
		return createBuffer;
	}

	public SqlInsert getInsertBuffer() {
		return insertBuffer;
	}

	public SqlUpdate getUpdateBuffer() {
		return updateBuffer;
	}

	public SqlSelect getSelectBuffer() {
		return selectBuffer;
	}

	public static class Builder extends BaseContext.Builder<JdbcContext, Builder> {
		private SqlCreateTable createBuffer;

		private SqlInsert insertBuffer;

		private SqlUpdate updateBuffer;

		private SqlSelect selectBuffer;

		public Builder createBuffer(SqlCreateTable createBuffer) {
			this.createBuffer = createBuffer;
			return this;
		}

		public Builder insertBuffer(SqlInsert insertBuffer) {
			this.insertBuffer = insertBuffer;
			return this;
		}

		public Builder updateBuffer(SqlUpdate updateBuffer) {
			this.updateBuffer = updateBuffer;
			return this;
		}

		public Builder selectBuffer(SqlSelect selectBuffer) {
			this.selectBuffer = selectBuffer;
			return this;
		}

		@Override
		protected final JdbcContext build(SqlSelect selectSource, List<MetaField> metaFields) {
			return new JdbcContext(
					selectSource,
					metaFields,
					createBuffer,
					insertBuffer,
					updateBuffer,
					selectBuffer
			);
		}
	}
}
