package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDriver implements DbDriver {

  protected static final String PART_COLUMN_NAME = "_partition";

  protected static final SqlNode PART_COLUMN =
      SqlUtils.createColumn(PART_COLUMN_NAME, SqlTypeName.INTEGER, 8, 0);

  private final SqlDialect dialect;

  protected final int partitionSize;

  public AbstractDriver(SqlDialect dialect, int partitionSize) {
    this.dialect = dialect;
    this.partitionSize = partitionSize;
  }

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

  protected abstract List<Partition> split(Connection connection, String table);

  private List<Column> getColumns(Connection connection, String table) {
    final List<String> columnNames = getColumnNames(connection, table);

    try (PreparedStatement statement =
        connection.prepareStatement(getSelectMinMax(table, columnNames))) {
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

  private String getSelectMinMax(String table, List<String> columnNames) {
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

  private List<String> getColumnNames(Connection connection, String table) {
    try (PreparedStatement statement = connection.prepareStatement(getSelectAll(table))) {
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

  protected final long getCount(Connection connection, String table) {
    return getCountSql(connection, getSelectCount(table, null));
  }

  protected final long getCount(
      Connection connection, String table, int partition, String partitionColumn) {
    final SqlNode where =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            new SqlNode[] {
              SqlUtils.getIdentifier(partitionColumn),
              SqlLiteral.createExactNumeric(String.valueOf(partition), SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO);

    return getCountSql(connection, getSelectCount(table, where));
  }

  private long getCountSql(Connection connection, String sql) {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery(sql)) {
        result.next();
        return result.getLong(1);
      }
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  protected final String getSelectAll(String table) {
    final SqlNode selectAll = new SqlIdentifier("*", SqlParserPos.ZERO);
    return getSelect(selectAll, table);
  }

  protected final String getSelectCount(String table, SqlNode where) {
    final SqlNode selectCount = createAggregator(SqlStdOperatorTable.COUNT, "*");
    return getSelect(SqlNodeList.of(selectCount), table, where);
  }

  private String getSelect(SqlNode singleSelect, String table) {
    return getSelect(SqlNodeList.of(singleSelect), table);
  }

  private String getSelect(SqlNodeList selectList, String table) {
    return getSelect(selectList, table, null);
  }

  private String getSelect(SqlNodeList selectList, String table, SqlNode where) {
    return toSql(
        new SqlSelect(
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
            null));
  }
}
