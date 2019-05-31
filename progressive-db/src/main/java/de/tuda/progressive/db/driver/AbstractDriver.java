package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public abstract class AbstractDriver implements DbDriver {

  private static final Logger log = LoggerFactory.getLogger(AbstractDriver.class);

  private static final String INSERT_PART_TPL =
      "insert into %s select %s from (%s) t where row_number = %d";

  private SqlDialect dialect;

  private int partitionSize;

  @Override
  public String toSql(SqlNode node) {
    return node.toSqlString(dialect).getSql();
  }

  @Override
  public SqlTypeName toSqlType(int jdbcType) {
    return null;
  }

  @Override
  public void prepareTable(Connection connection, String table, MetaData metaData) {
    final List<Partition> partitions = split(connection, table);
    final List<Column> columns = getColumns(connection, table);

    metaData.add(partitions, columns);
  }

  private List<Partition> split(Connection connection, String table) {
    final List<Partition> partitions = new ArrayList<>();

    for (Map.Entry<String, Integer> entry : getPartitionSizes(connection, table).entrySet()) {
      final String currentTable = entry.getKey();
      final int partitionCount = entry.getValue();

      log.info("create {} partitions for table {}", partitionCount, currentTable);
      createPartitions(connection, currentTable, partitionCount);

      log.info("insert data into table {}", currentTable);
      insertData(connection, currentTable, partitionCount);

      log.info("read meta data of table {}", currentTable);
      partitions.addAll(getPartitions(connection, currentTable, partitionCount));
    }

    return partitions;
  }

  private SortedMap<String, Integer> getPartitionSizes(Connection connection, String baseTable) {
    final List<String> foreignTables = getForeignTables(connection, baseTable);
    final SortedMap<String, Integer> partitionSizes = new TreeMap<>();

    if (partitionSize > 0) {
      partitionSizes.put(baseTable, getPartitionCount(connection, baseTable, partitionSize));

      for (String foreignTable : foreignTables) {
        partitionSizes.put(
            foreignTable, getPartitionCount(connection, foreignTable, partitionSize));
      }
    } else {
      log.info("get count of partitions of table {}", baseTable, partitionSize);
      throw new UnsupportedOperationException();
    }

    return partitionSizes;
  }

  private List<String> getForeignTables(Connection connection, String baseTable) {
    final List<String> foreignTables = new ArrayList<>();
    try (ResultSet result = connection.getMetaData().getImportedKeys(null, null, baseTable)) {
      while (result.next()) {
        foreignTables.add(result.getString("PKTABLE_NAME"));
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
    return foreignTables;
  }

  protected void createPartitions(Connection connection, String table, int partitions) {
    try (PreparedStatement srcStatement = connection.prepareStatement(toSql(getSelectAll(table)))) {
      final ResultSetMetaData metaData = srcStatement.getMetaData();

      try (Statement destStatement = connection.createStatement()) {
        for (int i = 0; i < partitions; i++) {
          final String partitionTable = getPartitionTable(table, i);
          final SqlCreateTable createTable =
              SqlUtils.createTable(this, metaData, null, partitionTable);

          dropTable(connection, partitionTable);
          destStatement.execute(toSql(createTable));
        }
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getPartitionTable(String table, int partition) {
    return String.format("%s_%d", getPartitionTable(table), partition);
  }

  protected void insertData(Connection connection, String table, int partitions) {
    final String template = String.format(getSelectTemplate(), partitions, table);
    insertData(connection, template, table, partitions);
  }

  private void insertData(Connection connection, String template, String table, int partitions) {
    try (PreparedStatement columnStatement =
        connection.prepareStatement(toSql(getSelectAll(table)))) {
      final SqlNodeList columns = SqlUtils.getColumns(columnStatement.getMetaData());

      try (Statement statement = connection.createStatement()) {
        for (int i = 0; i < partitions; i++) {
          final String sql =
              String.format(
                  INSERT_PART_TPL, getPartitionTable(table, i), toSql(columns), template, i);

          statement.execute(sql);
        }
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  protected abstract String getSelectTemplate();

  private int getPartitionCount(Connection connection, String table, int partitionSize) {
    log.info("get count of partitions of table {} with size {}", table, partitionSize);
    final long count = getCount(connection, table);
    return (int) Math.ceil(((double) count / (double) partitionSize));
  }

  private List<Column> getColumns(Connection connection, String baseTable) {
    final List<Column> columns = new ArrayList<>(getColumnsOfTable(connection, baseTable));

    for (String foreignTable : getForeignTables(connection, baseTable)) {
      columns.addAll(getColumnsOfTable(connection, foreignTable));
    }

    return columns;
  }

  private List<Column> getColumnsOfTable(Connection connection, String table) {
    final List<String> columnNames = getColumnNames(connection, table);

    try (PreparedStatement statement =
        connection.prepareStatement(toSql(getSelectMinMax(table, columnNames)))) {
      try (ResultSet result = statement.executeQuery()) {
        final List<Column> columns = new ArrayList<>();

        result.next();

        for (int i = 0; i < columnNames.size(); i++) {
          final Column column = new Column();
          final int pos = i * 2 + 1;

          column.setTable(table);
          column.setName(columnNames.get(i));
          column.setMin(result.getLong(pos));
          column.setMax(result.getLong(pos + 1));
          columns.add(column);
        }

        return columns;
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private SqlSelect getSelectMinMax(String table, List<String> columnNames) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    for (String columnName : columnNames) {
      selectList.add(createAggregator(SqlStdOperatorTable.MIN, columnName));
      selectList.add(createAggregator(SqlStdOperatorTable.MAX, columnName));
    }

    return getSelect(selectList, table);
  }

  private SqlBasicCall createAggregator(SqlAggFunction func, String columnName) {
    return new SqlBasicCall(
        func, new SqlNode[] {new SqlIdentifier(columnName, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  protected final void dropTable(Connection connection, String table) {
    try (Statement statement = connection.createStatement()) {
      final String sql = toSql(SqlUtils.dropTable(table));
      statement.execute(sql);
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private List<String> getColumnNames(Connection connection, String table) {
    try (PreparedStatement statement = connection.prepareStatement(toSql(getSelectAll(table)))) {
      final List<String> columnNames = new ArrayList<>();
      final ResultSetMetaData metaData = statement.getMetaData();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        switch (metaData.getColumnType(i)) {
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
            columnNames.add(metaData.getColumnName(i));
        }
      }

      return columnNames;
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private List<Partition> getPartitions(Connection connection, String table, int partitionCount) {
    final List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      final String partitionName = getPartitionTable(table, i);
      final Partition partition = new Partition();
      partition.setSrcTable(table);
      partition.setTableName(partitionName);
      partition.setId(i);
      partition.setEntries(getPartitionEntries(connection, table, i));
      partitions.add(partition);
    }
    return partitions;
  }

  protected long getPartitionEntries(Connection connection, String table, int partition) {
    return getCount(connection, getPartitionTable(table, partition), null);
  }

  protected final long getCount(Connection connection, String table) {
    return getCount(connection, table, null);
  }

  protected final long getCount(Connection connection, String table, SqlNode where) {
    return getCount(connection, getSelectCount(table, where));
  }

  protected final long getCount(Connection connection, SqlSelect select) {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery(toSql(select))) {
        result.next();
        return result.getLong(1);
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  protected final SqlSelect getSelectAll(String table) {
    final SqlNode selectAll = new SqlIdentifier("*", SqlParserPos.ZERO);
    return getSelect(selectAll, table);
  }

  private SqlSelect getSelectCount(String table, SqlNode where) {
    final SqlNode selectCount = createAggregator(SqlStdOperatorTable.COUNT, "*");
    return getSelect(SqlNodeList.of(selectCount), table, where);
  }

  private SqlSelect getSelect(SqlNode singleSelect, String table) {
    return getSelect(SqlNodeList.of(singleSelect), table);
  }

  private SqlSelect getSelect(SqlNodeList selectList, String table) {
    return getSelect(selectList, table, null);
  }

  private SqlSelect getSelect(SqlNodeList selectList, String table, SqlNode where) {
    return new SqlSelect(
        SqlParserPos.ZERO,
        new SqlNodeList(SqlParserPos.ZERO),
        selectList,
        new SqlIdentifier(table, SqlParserPos.ZERO),
        where,
        null,
        null,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        null,
        null);
  }

  public abstract static class Builder<D extends AbstractDriver, B extends Builder<D, B>> {
    private final SqlDialect dialect;

    private int partitionSize = -1;

    public Builder(SqlDialect dialect) {
      this.dialect = dialect;
    }

    public B partitionSize(int partitionSize) {
      this.partitionSize = partitionSize;
      return (B) this;
    }

    public abstract D build();

    protected D build(D driver) {
      ((AbstractDriver) driver).dialect = dialect;
      ((AbstractDriver) driver).partitionSize = partitionSize;
      return driver;
    }
  }
}
