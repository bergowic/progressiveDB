package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.ContextFactory;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.*;
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
      SqlSelectProgressive select,
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
      SqlSelectProgressive select,
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
      return MetaField.FUTURE_GROUP;
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
        case FUTURE_GROUP:
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
    SqlIdentifier from;
    if (sourceDriver.hasPartitions()) {
      from = SqlUtils.getIdentifier(sourceDriver.getPartitionTable(oldFrom.getSimple()));
    } else {
      from = oldFrom;
    }

    final Set<SqlIdentifier> futureWhereIdentifiers = createEmptyIdentifiers();
    final SqlBasicCall where = createWhere(futureWhereIdentifiers, select.getWhere());

    boolean hasAggregation = hasAggregation(metaFields);
    for (SqlIdentifier identifier : futureWhereIdentifiers) {
      metaFields.add(MetaField.FUTURE_WHERE);
      selectList.add(identifier);

      if (hasAggregation) {
        groups.add(identifier);
      }
    }

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

  private boolean hasAggregation(List<MetaField> metaFields) {
    for (MetaField metaField : metaFields) {
      switch (metaField) {
        case AVG:
        case COUNT:
        case SUM:
        case CONFIDENCE_INTERVAL:
          return true;
        case NONE:
        case PARTITION:
        case PROGRESS:
        case FUTURE_GROUP:
        case FUTURE_WHERE:
          // ignore
          break;
        default:
          throw new IllegalArgumentException("metaField not supported: " + metaField);
      }
    }
    return false;
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

  private SqlBasicCall createWhere(Set<SqlIdentifier> identifiers, SqlNode oldWhere) {
    final SqlBasicCall where = transformWhere(identifiers, oldWhere);
    if (!sourceDriver.hasPartitions()) {
      return where;
    }

    final SqlBasicCall eqPartition = createWhereEqPartition();
    if (where == null) {
      return eqPartition;
    }

    return new SqlBasicCall(
        SqlStdOperatorTable.AND, new SqlNode[] {where, eqPartition}, SqlParserPos.ZERO);
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

  private SqlBasicCall transformWhere(Set<SqlIdentifier> identifiers, SqlNode oldWhere) {
    if (oldWhere == null) {
      return null;
    }

    return get(identifiers, oldWhere, false, false);
  }

  private Set<SqlIdentifier> createEmptyIdentifiers() {
    return new TreeSet<>(
        (id1, id2) -> {
          for (int i = 0; i < Math.min(id1.names.size(), id2.names.size()); i++) {
            int compared = id1.names.get(i).compareTo(id2.names.get(i));
            if (compared != 0) {
              return compared;
            }
          }
          return id1.names.size() - id2.names.size();
        });
  }

  private SqlBasicCall get(
      Set<SqlIdentifier> identifiers, SqlNode node, boolean add, boolean inFuture) {
    if (node instanceof SqlFutureNode) {
      if (inFuture) {
        throw new IllegalArgumentException("future nodes must not be nested");
      }

      inFuture = true;
      node = ((SqlFutureNode) node).getNode();
    }

    final SqlBasicCall call = (SqlBasicCall) node;
    switch (call.getOperator().getName()) {
      case "AND":
        {
          boolean leftFuture = isFullFuture(call.getOperands()[0]);
          boolean rightFuture = isFullFuture(call.getOperands()[1]);
          boolean reverse = add && leftFuture && rightFuture;

          final SqlBasicCall left = get(identifiers, call.getOperands()[0], reverse, inFuture);
          final SqlBasicCall right = get(identifiers, call.getOperands()[1], reverse, inFuture);

          if (leftFuture) {
            addIdentifiers(identifiers, call.getOperands()[0]);
          }
          if (rightFuture) {
            addIdentifiers(identifiers, call.getOperands()[1]);
          }

          if (left == null) {
            return right;
          } else if (right == null) {
            return left;
          } else {
            return new SqlBasicCall(
                reverse ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND,
                new SqlNode[] {left, right},
                SqlParserPos.ZERO);
          }
        }
      case "OR":
        {
          boolean leftFuture = isFullFuture(call.getOperands()[0]);
          boolean rightFuture = isFullFuture(call.getOperands()[1]);
          boolean newAdd = add || (leftFuture ^ rightFuture);

          final SqlBasicCall left = get(identifiers, call.getOperands()[0], newAdd, inFuture);
          final SqlBasicCall right = get(identifiers, call.getOperands()[1], newAdd, inFuture);

          addIdentifiers(identifiers, call.getOperands()[0]);
          addIdentifiers(identifiers, call.getOperands()[1]);

          if (left == null) {
            return right;
          } else if (right == null) {
            return left;
          } else {
            return new SqlBasicCall(
                SqlStdOperatorTable.OR, new SqlNode[] {left, right}, SqlParserPos.ZERO);
          }
        }
      default:
        if (inFuture || add) {
          addIdentifiers(identifiers, call);
        }

        if (!inFuture || add) {
          return call;
        }
    }

    return null;
  }

  private boolean isFullFuture(SqlNode node) {
    if (node instanceof SqlFutureNode) {
      return true;
    } else if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      switch (call.getOperator().getName().toUpperCase()) {
        case "AND":
        case "OR":
          return isFullFuture(call.getOperands()[0]) && isFullFuture(call.getOperands()[1]);
        default:
          return false;
      }
    } else {
      throw new IllegalArgumentException("node type not expected: " + node);
    }
  }

  private void addIdentifiers(Set<SqlIdentifier> identifiers, SqlNode node) {
    if (node == null) {
      return;
    }

    if (node instanceof SqlBasicCall) {
      Arrays.stream(((SqlBasicCall) node).getOperands())
          .forEach(n -> addIdentifiers(identifiers, n));
    } else if (node instanceof SqlIdentifier) {
      identifiers.add((SqlIdentifier) node);
    } else if (node instanceof SqlFutureNode) {
      addIdentifiers(identifiers, ((SqlFutureNode) node).getNode());
    } else if (node instanceof SqlLiteral) {
      // ignore
    } else {
      throw new IllegalArgumentException("node type not expected: " + node);
    }
  }
}
