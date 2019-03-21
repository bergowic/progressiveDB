package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcContextFactory
    extends BaseContextFactory<JdbcSelectContext, JdbcSourceContext, JdbcDataBuffer> {

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
      Connection connection,
      SqlSelect select,
      Function<Pair<String, String>, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource) {
    final List<String> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields);
    final Map<Integer, Pair<Integer, Integer>> bounds = getBounds(columnMapper, metaFields, select);
    final String sql = sourceDriver.toSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = generateBufferTableName();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      final SqlSelect selectBuffer =
          getSelectBuffer(bufferFieldNames, bufferTableName, fieldNames, metaFields);

      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .metaFields(metaFields)
          .bounds(bounds)
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
  public JdbcSourceContext create(
      JdbcDataBuffer dataBuffer,
      SqlSelect select,
      Function<Pair<String, String>, Column> columnMapper) {
    final JdbcSelectContext context = dataBuffer.getContext();
    final Pair<SqlSelect, List<Integer>> transformed = transformSelect(context, select);

    final List<MetaField> metaFields =
        getMetaFields(context.getMetaFields(), transformed.getRight());
    final SqlSelect selectBuffer = transformed.getLeft();
    final Map<Integer, Pair<Integer, Integer>> bounds =
        getBounds(
            context::getFieldIndex, metaFields, context::getBound, selectBuffer.getSelectList());

    return new JdbcSourceContext.Builder()
        .metaFields(metaFields)
        .bounds(bounds)
        .selectSource(selectBuffer)
        .build();
  }

  @Override
  protected JdbcSelectContext create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<Pair<String, String>, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource) {
    final SqlSelect select = (SqlSelect) view.getQuery();
    final List<String> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields);
    final Map<Integer, Pair<Integer, Integer>> bounds = getBounds(columnMapper, metaFields, select);
    final String sql = sourceDriver.toSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = view.getName().getSimple();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      // TODO use correct select
      final SqlSelect selectBuffer =
          getSelectBuffer(bufferFieldNames, bufferTableName, fieldNames, metaFields);

      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .createBuffer(createBuffer)
          .selectSource(selectSource)
          .bounds(bounds)
          .selectBuffer(selectBuffer)
          .metaFields(metaFields)
          .fieldNames(fieldNames)
          .build();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private Map<Integer, Pair<Integer, Integer>> getBounds(
      Function<String, Integer> fieldMapper,
      List<MetaField> metaFields,
      Function<Integer, Pair<Integer, Integer>> boundMapper,
      SqlNodeList selectList) {
    final Map<Integer, Pair<Integer, Integer>> bounds = new HashMap<>();

    for (int i = 0; i < metaFields.size(); i++) {
      if (metaFields.get(i) == MetaField.CONFIDENCE_INTERVAL) {
        SqlNode node = selectList.get(i);
        String fieldName = null;

        if (node instanceof SqlBasicCall) {
          final SqlBasicCall call = (SqlBasicCall) node;

          if (SqlStdOperatorTable.AS.equals(call.getOperator())) {
            node = call.operand(1);
          }
        }

        if (node instanceof SqlIdentifier) {
          fieldName = ((SqlIdentifier) node).getSimple();
        }

        final int index = fieldMapper.apply(fieldName);
        bounds.put(i, boundMapper.apply(index));
      }
    }

    return bounds;
  }

  private Map<Integer, Pair<Integer, Integer>> getBounds(
      Function<Pair<String, String>, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect select) {
    final SqlNodeList selectList = select.getSelectList();
    final String table = ((SqlIdentifier) select.getFrom()).getSimple();
    final Map<Integer, Pair<Integer, Integer>> bounds = new HashMap<>();

    for (int i = 0; i < metaFields.size(); i++) {
      if (metaFields.get(i) == MetaField.CONFIDENCE_INTERVAL) {
        SqlBasicCall node = (SqlBasicCall) selectList.get(i);
        if (SqlStdOperatorTable.AS.equals(node.getOperator())) {
          node = node.operand(0);
        }

        final String columnName = ((SqlIdentifier) node.operand(0)).getSimple();
        final Column column = columnMapper.apply(ImmutablePair.of(table, columnName));
        bounds.put(i, ImmutablePair.of((int) column.getMin(), (int) column.getMax()));
      }
    }
    return bounds;
  }

  private Pair<SqlSelect, List<Integer>> transformSelect(
      JdbcBufferContext context, SqlSelect select) {
    final SqlNodeList selectList = SqlNodeList.clone(select.getSelectList());
    final List<Integer> indices =
        substituteFields(context::getFieldIndex, context.getMetaFields(), selectList);
    final SqlNodeList groups = getIndexColumns(context.getMetaFields(), indices);

    return ImmutablePair.of(
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            select.getFrom(),
            select.getWhere(),
            groups.size() > 0 ? groups : null,
            select.getHaving(),
            select.getWindowList(),
            select.getOrderList(),
            select.getOffset(),
            select.getFetch()),
        indices);
  }

  private List<Integer> substituteFields(
      Function<String, Integer> fieldMapper, List<MetaField> metaFields, SqlNodeList selectList) {
    final List<Integer> indices = new ArrayList<>();

    for (int i = 0; i < selectList.size(); i++) {
      final Pair<SqlNode, List<Integer>> substituted =
          substituteFields(fieldMapper, metaFields, selectList.get(i), true);

      selectList.set(i, substituted.getLeft());
      indices.addAll(substituted.getRight());
    }

    return indices;
  }

  private Pair<SqlNode, List<Integer>> substituteFields(
      Function<String, Integer> fieldMapper,
      List<MetaField> metaFields,
      SqlNode node,
      boolean addAlias) {
    final List<Integer> indices = new ArrayList<>();

    if (node instanceof SqlIdentifier) {
      final String fieldName = ((SqlIdentifier) node).getSimple();
      final int index = fieldMapper.apply(fieldName);
      if (index < 0) {
        throw new IllegalArgumentException("field not found: " + node);
      }

      final MetaField metaField = metaFields.get(index);
      switch (metaField) {
        case AVG:
          node =
              SqlUtils.createAvgAggregation(
                  SqlUtils.getIdentifier(getBufferFieldName(index, MetaField.SUM)),
                  SqlUtils.getIdentifier(getBufferFieldName(index, MetaField.COUNT)));
          break;
        case COUNT:
        case SUM:
          node =
              SqlUtils.createPercentAggregation(
                  index, SqlUtils.getIdentifier(getBufferFieldName(index, metaField)));
          break;
        case FUTURE:
        case NONE:
          node = SqlUtils.getIdentifier(getBufferFieldName(index, metaField));
          break;
        case PROGRESS:
        case PARTITION:
          node = new SqlDynamicParam(0, SqlParserPos.ZERO);
          break;
        case CONFIDENCE_INTERVAL:
          node =
              SqlUtils.createCast(
                  SqlUtils.getIdentifier(getBufferFieldName(index, metaField)), SqlTypeName.FLOAT);
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }

      if (addAlias) {
        node = SqlUtils.getAlias(node, fieldName);
      }

      indices.add(index);
    } else if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;

      if (SqlStdOperatorTable.AS.equals(call.getOperator())) {
        final Pair<SqlNode, List<Integer>> substituted =
            substituteFields(fieldMapper, metaFields, call.operand(0), false);

        call.setOperand(0, substituted.getLeft());
        indices.addAll(substituted.getRight());
      }

      // TODO
    }

    return ImmutablePair.of(node, indices);
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
        case AVG:
          fieldNames.add(getBufferFieldName(i, MetaField.SUM));
          fieldNames.add(getBufferFieldName(i++, MetaField.COUNT));
          break;
        case NONE:
        case COUNT:
        case SUM:
        case FUTURE:
          fieldNames.add(getBufferFieldName(i++, metaField));
          break;
        case PARTITION:
        case PROGRESS:
          i++;
          break;
        case CONFIDENCE_INTERVAL:
          fieldNames.add(getBufferFieldName(i++, metaField));
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }
    }

    return fieldNames;
  }

  private String getBufferFieldName(int index, MetaField metaField) {
    if (metaField == MetaField.NONE) {
      return String.format("f%d", index);
    } else {
      return String.format("f%d_%s", index, metaField.name());
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
      List<String> bufferFieldNames,
      String bufferTableName,
      List<String> fieldNames,
      List<MetaField> metaFields) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

    int i = 0;
    int index = 0;
    for (int j = 0; j < fieldNames.size(); j++) {
      final String alias = fieldNames.get(j);
      final MetaField metaField = metaFields.get(j);

      SqlNode newColumn = null;

      switch (metaField) {
        case NONE:
          newColumn = SqlUtils.getIdentifier(bufferFieldNames.get(i++));
          break;
        case AVG:
          newColumn =
              SqlUtils.createAvgAggregation(
                  SqlUtils.getIdentifier(bufferFieldNames.get(i)),
                  SqlUtils.getIdentifier(bufferFieldNames.get(i + 1)));
          i += 2;
          break;
        case COUNT:
        case SUM:
          newColumn =
              SqlUtils.createPercentAggregation(
                  index++, SqlUtils.getIdentifier(bufferFieldNames.get(i++)));
          break;
        case PARTITION:
          newColumn = SqlUtils.createFunctionMetaField(index++, SqlTypeName.INTEGER);
          break;
        case PROGRESS:
          newColumn = SqlUtils.createFunctionMetaField(index++, SqlTypeName.FLOAT);
          break;
        case FUTURE:
          // TODO remove
          break;
        case CONFIDENCE_INTERVAL:
          newColumn =
              SqlUtils.createCast(
                  SqlUtils.getIdentifier(bufferFieldNames.get(i++)), SqlTypeName.FLOAT);
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }

      if (newColumn != null) {
        selectList.add(SqlUtils.getAlias(newColumn, alias));
      }
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

      selectList.add(alias == null ? newColumn : SqlUtils.getAlias(newColumn, alias));
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
    return getIndexColumns(
        metaFields, IntStream.range(0, metaFields.size()).boxed().collect(Collectors.toList()));
  }

  private SqlNodeList getIndexColumns(List<MetaField> metaFields, List<Integer> indices) {
    final SqlNodeList indexColumns = new SqlNodeList(SqlParserPos.ZERO);
    for (Integer index : indices) {
      final MetaField metaField = metaFields.get(index);
      switch (metaField) {
        case NONE:
        case FUTURE:
          indexColumns.add(SqlUtils.getIdentifier(getBufferFieldName(index, metaField)));
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
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList indexColumns) {
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
