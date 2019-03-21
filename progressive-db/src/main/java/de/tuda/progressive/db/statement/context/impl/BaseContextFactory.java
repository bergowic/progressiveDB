package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.statement.context.ContextFactory;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class BaseContextFactory<
        C1 extends JdbcSourceContext, C2 extends BaseContext, D extends DataBuffer>
    implements ContextFactory<C1, C2, D> {

  protected final DbDriver sourceDriver;

  public BaseContextFactory(DbDriver sourceDriver) {
    this.sourceDriver = sourceDriver;
  }

  @Override
  public C1 create(
      Connection connection,
      SqlSelect select,
      Function<Pair<String, String>, Column> columnMapper) {
    final List<MetaField> metaFields = getMetaFields(select.getSelectList());
    final SqlSelect selectSource = transformSelect(select, metaFields);

    return create(connection, select, columnMapper, metaFields, selectSource);
  }

  @Override
  public C1 create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<Pair<String, String>, Column> columnMapper) {
    final SqlSelect select = (SqlSelect) view.getQuery();

    final List<MetaField> metaFields = getMetaFields(select.getSelectList());
    final SqlSelect selectSource = transformSelect(select, metaFields);

    return create(connection, view, columnMapper, metaFields, selectSource);
  }

  protected abstract C1 create(
      Connection connection,
      SqlSelect select,
      Function<Pair<String, String>, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource);

  protected abstract C1 create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<Pair<String, String>, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource);

  private <T> List<T> get(SqlNodeList columns, Function<SqlNode, T> func) {
    return StreamSupport.stream(columns.spliterator(), false)
        .map(func)
        .collect(Collectors.toList());
  }

  protected List<SqlIdentifier> getColumnAliases(SqlNodeList columns) {
    return get(columns, this::columnToAlias);
  }

  private SqlIdentifier columnToAlias(SqlNode column) {
    if (!(column instanceof SqlBasicCall)) {
      return null;
    }
    final SqlBasicCall call = (SqlBasicCall) column;
    if (call.getKind() != SqlKind.AS) {
      return null;
    }
    return (SqlIdentifier) call.operand(1);
  }

  protected final List<MetaField> getMetaFields(List<MetaField> metaFields, List<Integer> indices) {
    return indices.stream().map(metaFields::get).collect(Collectors.toList());
  }

  protected final List<MetaField> getMetaFields(SqlNodeList columns) {
    return get(columns, this::columnToMetaField);
  }

  private MetaField columnToMetaField(SqlNode column) {
    if (column instanceof SqlIdentifier || column instanceof SqlLiteral) {
      return MetaField.NONE;
    }
    if (column instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) column;
      SqlOperator operator = call.getOperator();

      switch (operator.getName().toUpperCase()) {
        case "AVG":
          return MetaField.AVG;
        case "COUNT":
          return MetaField.COUNT;
        case "SUM":
          return MetaField.SUM;
        case "AS":
          return columnToMetaField(call.getOperands()[0]);
        case "PROGRESSIVE_PARTITION":
          return MetaField.PARTITION;
        case "PROGRESSIVE_PROGRESS":
          return MetaField.PROGRESS;
        case "PROGRESSIVE_CONFIDENCE":
          return MetaField.CONFIDENCE_INTERVAL;
      }

      throw new IllegalArgumentException("operation is not supported: " + operator.getName());
    }
    if (column instanceof SqlFutureNode) {
      return MetaField.FUTURE;
    }

    throw new IllegalArgumentException("column type is not supported: " + column.getClass());
  }

  protected final SqlSelect transformSelect(SqlSelect select, List<MetaField> metaFields) {
    final SqlNodeList oldSelectList = select.getSelectList();
    final SqlNodeList oldGroups = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();

    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNodeList groups = new SqlNodeList(SqlParserPos.ZERO);

    for (int i = 0; i < oldSelectList.size(); i++) {
      SqlBasicCall call;
      switch (metaFields.get(i)) {
        case AVG:
          call = (SqlBasicCall) oldSelectList.get(i);
          SqlBasicCall avg;
          if (call.getKind() == SqlKind.AS) {
            avg = (SqlBasicCall) call.getOperands()[0];
          } else {
            avg = call;
          }

          selectList.add(SqlUtils.createSumAggregation(avg.getOperands()));
          selectList.add(SqlUtils.createCountAggregation(avg.getOperands()));
          break;
        case COUNT:
        case SUM:
        case NONE:
          selectList.add(oldSelectList.get(i));
          break;
        case PARTITION:
        case PROGRESS:
          // don't add anything
          break;
        case FUTURE:
          selectList.add(removeFuture(oldSelectList.get(i)));
          break;
        case CONFIDENCE_INTERVAL:
          call = (SqlBasicCall) oldSelectList.get(i);
          selectList.add(SqlUtils.createCountAggregation(call.getOperands()));
          break;
        default:
          throw new IllegalArgumentException("metaField not supported: " + metaFields.get(i));
      }
    }

    for (SqlNode group : oldGroups) {
      if (group instanceof SqlFutureNode) {
        groups.add(((SqlFutureNode) group).getNode());
      } else {
        groups.add(group);
      }
    }

    final SqlIdentifier oldFrom = (SqlIdentifier) select.getFrom();
    final SqlIdentifier from =
        SqlUtils.getIdentifier(sourceDriver.getPartitionTable(oldFrom.getSimple()));
    final SqlBasicCall where = createWhere((SqlBasicCall) select.getWhere());

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        from,
        where,
        groups.size() > 0 ? groups : null,
        select.getHaving(),
        select.getWindowList(),
        select.getOrderList(),
        select.getOffset(),
        select.getFetch());
  }

  protected final SqlNodeList removeFutures(SqlNodeList nodes) {
    if (nodes == null) {
      return null;
    }

    return new SqlNodeList(
        StreamSupport.stream(nodes.spliterator(), false)
            .map(this::removeFuture)
            .collect(Collectors.toList()),
        SqlParserPos.ZERO);
  }

  protected final SqlNode removeFuture(SqlNode node) {
    if (node instanceof SqlFutureNode) {
      return ((SqlFutureNode) node).getNode();
    }

    if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      final SqlNode[] operands = call.getOperands();

      if (call.getOperator() instanceof SqlAsOperator) {
        return new SqlBasicCall(
            SqlStdOperatorTable.AS,
            new SqlNode[] {removeFuture(operands[0]), operands[1]},
            SqlParserPos.ZERO);
      }
    }

    return node;
  }

  private SqlBasicCall createWhere(SqlBasicCall oldWhere) {
    final SqlBasicCall eqPartition = createWhereEqPartition();
    if (oldWhere == null) {
      return eqPartition;
    }

    return new SqlBasicCall(
        SqlStdOperatorTable.AND, new SqlNode[] {oldWhere, eqPartition}, SqlParserPos.ZERO);
  }

  private SqlBasicCall createWhereEqPartition() {
    return new SqlBasicCall(
        SqlStdOperatorTable.EQUALS,
        new SqlNode[] {
          new SqlIdentifier(Collections.singletonList("_partition"), SqlParserPos.ZERO),
          new SqlDynamicParam(0, SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }
}
