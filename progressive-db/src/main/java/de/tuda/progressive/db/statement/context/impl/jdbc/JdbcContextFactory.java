package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class JdbcContextFactory
    extends BaseContextFactory<JdbcSelectContext, JdbcSelectContext, JdbcDataBuffer> {

  private final DbDriver bufferDriver;

  public JdbcContextFactory(DbDriver sourceDriver, DbDriver bufferDriver) {
    super(sourceDriver);

    if (!bufferDriver.hasUpsert()) {
      throw new IllegalArgumentException("driver does not support upsert");
    }

    this.bufferDriver = bufferDriver;
  }

  @Override
  protected JdbcSelectContext create(
      Connection connection, SqlSelect select, List<MetaField> metaFields, SqlSelect selectSource) {
    final List<String> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields);
    final String sql = sourceDriver.toSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = generateBufferTableName();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      final SqlSelect selectBuffer =
          getSelectBuffer(metaData, bufferFieldNames, bufferTableName, fieldNames, metaFields);

      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .metaFields(metaFields)
          .selectSource(selectSource)
          .fieldNames(fieldNames)
          .createBuffer(createBuffer)
          .selectBuffer(selectBuffer)
          .build();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public JdbcSelectContext create(JdbcDataBuffer dataBuffer, SqlSelect select) {
    return null;
  }

  @Override
  protected JdbcSelectContext create(
      Connection connection,
      SqlCreateProgressiveView view,
      List<MetaField> metaFields,
      SqlSelect selectSource) {
    final SqlSelect select = (SqlSelect) view.getQuery();
    final List<String> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields);
    final String sql = sourceDriver.toSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = view.getName().getSimple();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .createBuffer(createBuffer)
          .selectSource(selectSource)
          .metaFields(metaFields)
          .fieldNames(fieldNames)
          .build();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private List<String> getFieldNames(SqlNodeList selectList) {
    final List<String> fieldNames = new ArrayList<>(selectList.size());
    for (SqlNode select : selectList) {
      String name = null;
      if (select instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) select;

        if (SqlStdOperatorTable.AS.equals(call.getOperator())) {
          select = call.operands[1];
        } else {
          name = call.getOperator().getName();
        }
      } else if (select instanceof SqlFutureNode) {
        SqlFutureNode futureNode = (SqlFutureNode) select;
        select = futureNode.getNode();
      }

      if (name == null) {
        name = select.toString();
      }
      fieldNames.add(name);
    }
    return fieldNames;
  }

  private List<String> getBufferFieldNames(List<MetaField> metaFields) {
    final List<String> fieldNames = new ArrayList<>();
    int i = 0;

    for (MetaField metaField : metaFields) {
      switch (metaField) {
        case NONE:
          fieldNames.add(getBufferFieldName(i++));
          break;
        case AVG:
          fieldNames.add(getBufferFieldName(i, "sum"));
          fieldNames.add(getBufferFieldName(i++, "count"));
          break;
        case COUNT:
          fieldNames.add(getBufferFieldName(i++, "count"));
          break;
        case SUM:
          fieldNames.add(getBufferFieldName(i++, "sum"));
          break;
        case FUTURE:
          fieldNames.add(getBufferFieldName(i++, "future"));
        case PARTITION:
        case PROGRESS:
          i++;
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }
    }

    return fieldNames;
  }

  private String getBufferFieldName(int index) {
    return getBufferFieldName(index, null);
  }

  private String getBufferFieldName(int index, String suffix) {
    if (suffix == null) {
      return String.format("f%d", index);
    } else {
      return String.format("f%d_%s", index, suffix);
    }
  }

  private String generateBufferTableName() {
    return "progressive_buffer_" + UUID.randomUUID().toString().replaceAll("-", "_");
  }

  private SqlCreateTable getCreateBuffer(
      ResultSetMetaData metaData,
      List<String> bufferFieldNames,
      String bufferTableName,
      SqlNodeList columnIndexes) {
    SqlNode[] additionalColumns;

    if (columnIndexes.size() > 0) {
      additionalColumns =
          new SqlNode[] {
            SqlDdlNodes.primary(
                SqlParserPos.ZERO,
                new SqlIdentifier("pk_" + bufferTableName, SqlParserPos.ZERO),
                columnIndexes)
          };
    } else {
      additionalColumns = new SqlNode[0];
    }

    return SqlUtils.createTable(
        bufferDriver, metaData, bufferFieldNames, bufferTableName, additionalColumns);
  }

  private SqlSelect getSelectBuffer(
      ResultSetMetaData metaData,
      List<String> bufferFieldNames,
      String bufferTableName,
      List<String> fieldNames,
      List<MetaField> metaFields) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

    int i = 0;
    int index = 0;
    for (int j = 0; j < fieldNames.size(); j++) {
      final SqlIdentifier alias = new SqlIdentifier(fieldNames.get(j), SqlParserPos.ZERO);
      final MetaField metaField = metaFields.get(j);

      SqlNode newColumn;

      switch (metaField) {
        case NONE:
          newColumn = SqlUtils.getIdentifier(bufferFieldNames.get(i));
          i++;
          break;
        case AVG:
          newColumn =
              SqlUtils.createAvgAggregation(
                  SqlUtils.getIdentifier(bufferFieldNames.get(i)),
                  SqlUtils.getIdentifier(bufferFieldNames.get(i + 1)));
          i += 2;
          break;
        case COUNT:
          newColumn =
              SqlUtils.createPercentAggregation(
                  index, SqlUtils.getIdentifier(bufferFieldNames.get(i)));
          i++;
          index++;
          break;
        case SUM:
          newColumn =
              SqlUtils.createPercentAggregation(
                  index, SqlUtils.getIdentifier(bufferFieldNames.get(i)));
          i++;
          index++;
          break;
        case PARTITION:
          newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.INTEGER);
          index++;
          break;
        case PROGRESS:
          newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.FLOAT);
          index++;
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }

      selectList.add(
          new SqlBasicCall(
              SqlStdOperatorTable.AS, new SqlNode[] {newColumn, alias}, SqlParserPos.ZERO));
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private SqlSelect getSelectBufferForView(
      ResultSetMetaData metaData,
      String bufferTableName,
      List<SqlIdentifier> columnAliases,
      List<MetaField> metaFields) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

    int i = 0;
    int index = 0;
    for (int j = 0; j < columnAliases.size(); j++) {
      final SqlIdentifier alias = columnAliases.get(j);
      final MetaField metaField = metaFields.get(j);

      SqlNode newColumn;

      switch (metaField) {
        case NONE:
          newColumn = SqlUtils.getColumnIdentifier(metaData, i + 1);
          i++;
          break;
        case AVG:
          newColumn =
              SqlUtils.createAvgAggregation(
                  SqlUtils.getColumnIdentifier(metaData, i + 1),
                  SqlUtils.getColumnIdentifier(metaData, i + 2));
          i += 2;
          break;
        case COUNT:
          newColumn =
              SqlUtils.createPercentAggregation(
                  index, SqlUtils.getColumnIdentifier(metaData, i + 1));
          i++;
          index++;
          break;
        case SUM:
          newColumn =
              SqlUtils.createPercentAggregation(
                  index, SqlUtils.getColumnIdentifier(metaData, i + 1));
          i++;
          index++;
          break;
        case PARTITION:
          newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.INTEGER);
          index++;
          break;
        case PROGRESS:
          newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.FLOAT);
          index++;
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }

      selectList.add(
          alias == null
              ? newColumn
              : new SqlBasicCall(
                  SqlStdOperatorTable.AS, new SqlNode[] {newColumn, alias}, SqlParserPos.ZERO));
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private SqlNodeList getIndexColumns(List<MetaField> metaFields) {
    final SqlNodeList indexColumns = new SqlNodeList(SqlParserPos.ZERO);
    for (int i = 0; i < metaFields.size(); i++) {
      switch (metaFields.get(i)) {
        case NONE:
        case FUTURE:
          indexColumns.add(new SqlIdentifier(getBufferFieldName(i), SqlParserPos.ZERO));
          break;
      }
    }
    return indexColumns;
  }

  private JdbcSelectContext.Builder builder(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList indexColumns)
      throws SQLException {
    return new JdbcSelectContext.Builder()
        .insertBuffer(getInsertBuffer(bufferFieldNames, bufferTableName, indexColumns))
        .updateBuffer(getUpdateBuffer(bufferFieldNames, bufferTableName, indexColumns));
  }

  private SqlInsert getInsertBuffer(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList indexColumns)
      throws SQLException {
    final int count = bufferFieldNames.size();

    final SqlIdentifier targetTable = new SqlIdentifier(bufferTableName, SqlParserPos.ZERO);
    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNode[] insertValues = new SqlNode[count];
    final SqlNodeList updateValues = new SqlNodeList(SqlParserPos.ZERO);

    for (int i = 0; i < count; i++) {
      final SqlNode param = new SqlDynamicParam(count + i, SqlParserPos.ZERO);
      final SqlIdentifier column = new SqlIdentifier(bufferFieldNames.get(i), SqlParserPos.ZERO);

      columns.add(column);
      insertValues[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);

      if (isKey(bufferFieldNames.get(i), indexColumns)) {
        updateValues.add(param);
      } else {
        updateValues.add(
            new SqlBasicCall(
                SqlStdOperatorTable.PLUS, new SqlNode[] {column, param}, SqlParserPos.ZERO));
      }
    }

    if (indexColumns.size() > 0) {
      return new SqlUpsert(
          SqlParserPos.ZERO,
          targetTable,
          columns,
          insertValues,
          indexColumns,
          new SqlUpdate(
              SqlParserPos.ZERO,
              new SqlIdentifier(Collections.emptyList(), SqlParserPos.ZERO),
              columns,
              updateValues,
              null,
              null,
              null));
    } else {
      return new SqlInsert(
          SqlParserPos.ZERO,
          SqlNodeList.EMPTY,
          targetTable,
          SqlUtils.getValues(insertValues),
          columns);
    }
  }

  private boolean isKey(String column, SqlNodeList groups) {
    if (groups == null) {
      return false;
    }

    for (SqlNode group : groups) {
      if (group instanceof SqlIdentifier) {
        if (column.equalsIgnoreCase(((SqlIdentifier) group).getSimple())) {
          return true;
        }
      }
    }
    return false;
  }

  private SqlUpdate getUpdateBuffer(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList columnIndexes) {
    if (bufferDriver.hasUpsert() && columnIndexes.size() > 0) {
      return null;
    }

    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNodeList values = new SqlNodeList(SqlParserPos.ZERO);

    if (columnIndexes.size() != 0) {
      // TODO implement if driver does not support upsert
      throw new IllegalStateException("driver does not support upsert");
    }

    for (int i = 0; i < bufferFieldNames.size(); i++) {
      final SqlNode param = new SqlDynamicParam(i, SqlParserPos.ZERO);
      final SqlIdentifier column = new SqlIdentifier(bufferFieldNames.get(i), SqlParserPos.ZERO);
      columns.add(column);

      if (isKey(bufferFieldNames.get(i), columnIndexes)) {
        values.add(param);
      } else {
        values.add(
            new SqlBasicCall(
                SqlStdOperatorTable.PLUS, new SqlNode[] {column, param}, SqlParserPos.ZERO));
      }
    }

    return new SqlUpdate(
        SqlParserPos.ZERO,
        new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
        columns,
        values,
        null,
        null,
        null);
  }
}
