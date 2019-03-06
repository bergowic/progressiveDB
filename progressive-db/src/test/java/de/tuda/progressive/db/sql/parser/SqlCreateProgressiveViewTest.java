package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlCreateProgressiveViewTest {

  private SqlParser.ConfigBuilder configBuilder;

  @BeforeEach
  void init() {
    configBuilder =
        SqlParser.configBuilder()
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(SqlParserImpl.FACTORY);
  }

  void testSimple(boolean replace) throws Exception {
    final String name = "a";
    final String table = "b";

    SqlParser parser =
        SqlParser.create(
            String.format(
                "create %s progressive view %s as select * from %s",
                replace ? "or replace" : "", name, table),
            configBuilder.build());
    SqlNode node = parser.parseStmt();

    assertEquals(SqlCreateProgressiveView.class, node.getClass());

    SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

    assertEquals(name, createProgressiveView.getName().getSimple());
    assertEquals(replace, createProgressiveView.getReplace());
    assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

    SqlSelect select = (SqlSelect) createProgressiveView.getQuery();

    assertEquals(SqlIdentifier.class, select.getFrom().getClass());

    SqlIdentifier from = (SqlIdentifier) select.getFrom();

    assertEquals(table, from.getSimple());
  }

  @Test
  void simpleCreate() throws Exception {
    testSimple(false);
  }

  @Test
  void simpleCreateOrReplace() throws Exception {
    testSimple(true);
  }

  private String buildGroup(Triple<String, Boolean, String> column) {
    String result = column.getLeft();
    if (column.getMiddle()) {
      result += " future";
    }
    return result;
  }

  private String buildSelect(Triple<String, Boolean, String> column) {
    String result = buildGroup(column);
    final String alias = column.getRight();
    if (alias != null) {
      result += " as " + alias;
    }
    return result;
  }

  private void testFutureGroupBy(Triple<String, Boolean, String>... columns) throws Exception {
    final String name = "a";
    final String table = "b";
    final List<String> selects =
        Arrays.stream(columns).map(this::buildSelect).collect(Collectors.toList());
    final List<String> groupBy =
        Arrays.stream(columns).map(this::buildGroup).collect(Collectors.toList());

    final String sql =
        String.format(
            "create progressive view %s as select %s from %s group by %s",
            name, "x, " + String.join(", ", selects), table, String.join(", ", groupBy));

    SqlParser parser = SqlParser.create(sql, configBuilder.build());
    SqlNode node = parser.parseStmt();

    assertEquals(SqlCreateProgressiveView.class, node.getClass());

    SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

    assertEquals(name, createProgressiveView.getName().getSimple());
    assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

    SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
    SqlNodeList selectList = select.getSelectList();
    SqlNodeList groupByList = select.getGroup();

    assertEquals(columns.length, groupByList.size());
    for (int i = 0; i < columns.length; i++) {
      SqlIdentifier groupIdentifier;

      if (columns[i].getMiddle()) {
        final String alias = columns[i].getRight();
        if (alias != null) {
          assertAlias(columns[i], selectList.get(i + 1));
        }

        assertEquals(SqlFutureNode.class, groupByList.get(i).getClass());
        groupIdentifier = (SqlIdentifier) ((SqlFutureNode) groupByList.get(i)).getNode();
      } else {
        assertEquals(SqlIdentifier.class, groupByList.get(i).getClass());
        groupIdentifier = (SqlIdentifier) groupByList.get(i);
      }

      assertEquals(columns[i].getLeft(), groupIdentifier.getSimple());
    }
  }

  private void assertAlias(Triple<String, Boolean, String> column, SqlNode alias) {
    assertEquals(SqlBasicCall.class, alias.getClass());

    final SqlBasicCall selectCall = (SqlBasicCall) alias;
    assertEquals(SqlAsOperator.class, selectCall.getOperator().getClass());

    SqlNode[] operands = selectCall.getOperands();

    assertEquals(SqlFutureNode.class, operands[0].getClass());
    final SqlNode node = ((SqlFutureNode) operands[0]).getNode();

    assertEquals(SqlIdentifier.class, node.getClass());
    assertEquals(column.getLeft(), ((SqlIdentifier) node).getSimple());

    assertEquals(SqlIdentifier.class, operands[1].getClass());
    assertEquals(column.getRight(), ((SqlIdentifier) operands[1]).getSimple());
  }

  @Test
  void groupBy() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", false, null));
  }

  @Test
  void groupByFuture() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", true, null));
  }

  @Test
  void groupByMultiple() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", false, null), new ImmutableTriple<>("b", false, null));
  }

  @Test
  void groupByMultipleMixed() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", false, null), new ImmutableTriple<>("b", true, null));
  }

  @Test
  void groupByMultipleFuture() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", true, null), new ImmutableTriple<>("b", true, null));
  }

  @Test
  void groupByFutureAlias() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", true, "foo"));
  }
}
