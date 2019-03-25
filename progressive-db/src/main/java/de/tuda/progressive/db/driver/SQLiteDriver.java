package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;

public class SQLiteDriver extends AbstractDriver {

  public static final DbDriver INSTANCE = new SQLiteDriver();

  public static final SqlDialect SQL_DIALECT =
      new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT.withIdentifierQuoteString("\""));

  public SQLiteDriver() {
    super(SQL_DIALECT, -1);
  }

  @Override
  public SqlTypeName toSqlType(int jdbcType) {
    switch (jdbcType) {
      case Types.NULL:
        return SqlTypeName.VARCHAR;
    }
    return null;
  }

  @Override
  protected List<Partition> split(Connection connection, String table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPartitionTable(String table) {
    // TODO
    return table;
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }
}
