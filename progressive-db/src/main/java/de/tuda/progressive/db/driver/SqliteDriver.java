package de.tuda.progressive.db.driver;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;

public class SqliteDriver extends AbstractDriver {

	public static final DbDriver INSTANCE = new SqliteDriver();

	public SqliteDriver() {
		super(new AnsiSqlDialect(
				SqlDialect.EMPTY_CONTEXT
						.withIdentifierQuoteString("\"")
		));
	}

	@Override
	public SqlTypeName toSqlType(int jdbcType) {
		switch (jdbcType) {
			case Types.NULL:
				return SqlTypeName.VARCHAR;
		}
		return null;
	}
}
