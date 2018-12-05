package de.tuda.progressive.db.driver;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public class AbstractDriver implements DbDriver {

	private final SqlDialect dialect;

	public AbstractDriver(SqlDialect dialect) {
		this.dialect = dialect;
	}

	@Override
	public String toSql(SqlNode node) {
		return node.toSqlString(dialect).getSql();
	}

	@Override
	public SqlTypeName toSqlType(int jdbcType) {
		return null;
	}
}
