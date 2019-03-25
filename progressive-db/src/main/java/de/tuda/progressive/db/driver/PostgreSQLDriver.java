package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.meta.jdbc.JdbcMetaData;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PostgreSQLDriver extends PartitionDriver {

  private static final Logger log = LoggerFactory.getLogger(PostgreSQLDriver.class);

  private static SqlDialect SQL_DIALECT =
      new PostgresqlSqlDialect(
          PostgresqlSqlDialect.EMPTY_CONTEXT.withDatabaseProduct(
              SqlDialect.DatabaseProduct.POSTGRESQL));

  private static final int PARTITION_SIZE = 200000;

  public static void main(String[] args) throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/progressive", "postgres", "postgres")) {
      PostgreSQLDriver driver = new PostgreSQLDriver();
      MetaData metaData = new JdbcMetaData("jdbc:sqlite:C:/tmp/progressive.sqlite");
      driver.prepareTable(connection, "ONTIME1M", metaData);
    }
  }

  private static final String PART_DEF = String.format("partition by list(%s)", PART_COLUMN_NAME);

  private static final String PARTITION_TPL = "create table %s partition of %s for values in (%d)";
  private static final String INSERT_FROM_TPL =
      "insert into %s select t.*, row_number() over() %% %d from %s t";

  public PostgreSQLDriver() {
    this(SQL_DIALECT, PARTITION_SIZE);
  }

  public PostgreSQLDriver(SqlDialect dialect) {
    this(dialect, PARTITION_SIZE);
  }

  public PostgreSQLDriver(int partitionSize) {
    this(SQL_DIALECT, partitionSize);
  }

  public PostgreSQLDriver(SqlDialect dialect, int partitionSize) {
    super(dialect, partitionSize);
  }

  @Override
  public String getPartitionTable(String table) {
    return table + PART_COLUMN_NAME;
  }

  @Override
  protected void createPartitions(Connection connection, String table, int partitions) {
    final String partitionTable = getPartitionTable(table);

    createPartitionTable(connection, table, partitionTable);

    for (int i = 0; i < partitions; i++) {
      final String partitionName = getPartitionTable(table, i);
      log.info("create partition: {}", partitionName);
      createPartition(connection, partitionTable, partitionName, i);
    }
  }

  @Override
  protected void insertData(Connection connection, String table, int partitions) {
    insertData(INSERT_FROM_TPL, connection, table, getPartitionTable(table), partitions);
  }

  private void createPartitionTable(Connection connection, String srcTable, String destTable) {
    try (PreparedStatement srcStatement = connection.prepareStatement(getSelectAll(srcTable))) {
      final ResultSetMetaData metaData = srcStatement.getMetaData();
      final SqlCreateTable createTable =
          SqlUtils.createTable(this, metaData, null, destTable, PART_COLUMN);
      final String createTableSql = String.format("%s %s", toSql(createTable), PART_DEF);

      try (Statement destStatement = connection.createStatement()) {
        destStatement.execute(createTableSql);
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private void createPartition(
      Connection connection, String parentTable, String partitionTable, int id) {
    try (Statement statement = connection.createStatement()) {
      final String sql = String.format(PARTITION_TPL, partitionTable, parentTable, id);
      statement.execute(sql);
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }
}
