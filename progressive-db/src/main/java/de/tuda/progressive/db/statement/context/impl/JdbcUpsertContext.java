package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

public class JdbcUpsertContext extends JdbcContext {

	private final SqlUpsert upsertBuffer;

	public JdbcUpsertContext(
			SqlSelect selectSource,
			List<MetaField> metaFields,
			SqlCreateTable createBuffer,
			SqlSelect selectBuffer,
			SqlUpsert upsertBuffer
	) {
		super(selectSource, metaFields, createBuffer, selectBuffer);
		this.upsertBuffer = upsertBuffer;
	}

	public SqlUpsert getUpsertBuffer() {
		return upsertBuffer;
	}

	public static class Builder extends JdbcContext.Builder<JdbcUpsertContext, Builder> {
		private SqlUpsert upsertBuffer;

		public Builder upsertBuffer(SqlUpsert upsertBuffer) {
			this.upsertBuffer = upsertBuffer;
			return this;
		}

		@Override
		protected JdbcUpsertContext build(
				SqlSelect selectSource,
				List<MetaField> metaFields,
				SqlCreateTable createBuffer,
				SqlSelect selectBuffer
		) {
			return new JdbcUpsertContext(selectSource, metaFields, createBuffer, selectBuffer, upsertBuffer);
		}
	}
}
