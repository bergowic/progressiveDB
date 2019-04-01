package de.tuda.progressive.db.driver;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.Types;

public class SQLiteDriver extends AbstractDriver {

  public static final DbDriver INSTANCE = new SQLiteDriver();

  public static final SqlDialect SQL_DIALECT =
      new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT.withIdentifierQuoteString("\""));

  private SQLiteDriver() {}

  @Override
  public SqlTypeName toSqlType(int jdbcType) {
    switch (jdbcType) {
      case Types.NULL:
        return SqlTypeName.VARCHAR;
    }
    return null;
  }

  @Override
  protected void insertData(Connection connection, String table, int partitions) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPartitionTable(String table) {
    return table;
  }

  @Override
  protected String getSelectTemplate() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasPartitions() {
    return false;
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }

  public static class Builder extends AbstractDriver.Builder<SQLiteDriver, Builder> {
    public Builder() {
      this(SQL_DIALECT);
    }

    public Builder(SqlDialect dialect) {
      super(dialect);
    }

    @Override
    public SQLiteDriver build() {
      return build(new SQLiteDriver());
    }
  }
}
