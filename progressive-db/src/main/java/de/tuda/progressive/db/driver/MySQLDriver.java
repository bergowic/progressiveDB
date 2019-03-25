package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.sql.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MySQLDriver extends PartitionDriver {

  private static SqlDialect SQL_DIALECT =
      new MysqlSqlDialect(
          MysqlSqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL));

  private static final int PARTITION_SIZE = 500000;

  private static final String PART_DEF =
      String.format("partition by list(%s) (%%s)", PART_COLUMN_NAME);
  private static final String PART_SINGLE_TPL = "partition %s values in (%d)";

  private static final String INSERT_FROM_TPL =
      "insert into %s select t.*, (@row_number := @row_number + 1) %% %d from %s t, (SELECT @row_number := 0) rn";

  public MySQLDriver() {
    this(SQL_DIALECT);
  }

  public MySQLDriver(SqlDialect dialect) {
    super(dialect, PARTITION_SIZE);
  }

  public MySQLDriver(int partitionSize) {
    this(SQL_DIALECT, partitionSize);
  }

  public MySQLDriver(SqlDialect dialect, int partitionSize) {
    super(dialect, partitionSize);
  }

  @Override
  public String getPartitionTable(String table) {
    return table + PART_COLUMN_NAME;
  }

  @Override
  protected void createPartitions(Connection connection, String table, int partitions) {
    try (PreparedStatement srcStatement = connection.prepareStatement(getSelectAll(table))) {
      final ResultSetMetaData metaData = srcStatement.getMetaData();
      final SqlCreateTable createTable =
          SqlUtils.createTable(this, metaData, null, getPartitionTable(table), PART_COLUMN);
      final String partitionsDef =
          String.format(
              PART_DEF,
              String.join(
                  ", ",
                  IntStream.range(0, partitions)
                      .mapToObj(i -> String.format(PART_SINGLE_TPL, getPartitionTable(table, i), i))
                      .collect(Collectors.toList())));
      final String createTableSql = String.format("%s %s", toSql(createTable), partitionsDef);

      try (Statement destStatement = connection.createStatement()) {
        destStatement.execute(createTableSql);
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void insertData(Connection connection, String table, int partitions) {
    insertData(INSERT_FROM_TPL, connection, table, getPartitionTable(table), partitions);
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    MySQLDriver driver = new MySQLDriver();
    try (Connection connection =
        DriverManager.getConnection("jdbc:mysql://localhost:3306/progressive", "root", "")) {
      driver.split(connection, "_test");
    }
  }
}
