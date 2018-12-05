package de.tuda.progressive.db.driver;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public interface DbDriver {

	String toSql(SqlNode node);

	SqlTypeName toSqlType(int jdbcType);
}
