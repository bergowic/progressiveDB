package de.tuda.progressive.db.sql.parser;

import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlSelectProgressiveTest {

  private SqlParser.Config config;

  @BeforeEach
  void init() {
    config =
        SqlParser.configBuilder()
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
  }

  private void test(String sql, SqlNodeList selectList, SqlNode from, SqlNodeList withFutureGroupBy)
      throws Exception {
    final SqlNode node = SqlParser.create(sql, config).parseQuery();

    assertEquals(SqlSelectProgressive.class, node.getClass());

    final SqlSelectProgressive select = (SqlSelectProgressive) node;

    assertTrue(selectList.equalsDeep(select.getSelectList(), Litmus.THROW));
    assertTrue(from.equalsDeep(select.getFrom(), Litmus.THROW));

    if (withFutureGroupBy == null) {
      assertNull(select.getWithFutureGroupBy());
    } else {
      assertTrue(withFutureGroupBy.equalsDeep(select.getWithFutureGroupBy(), Litmus.THROW));
    }
  }

  @Test
  void testProgressiveKeyword() throws Exception {
    test(
        "select progressive * from t",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        null);
  }

  @Test
  void testWithFutureGroup() throws Exception {
    test(
        "select progressive * from t with future group by a",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        SqlNodeList.of(SqlUtils.getIdentifier("a")));
  }

  @Test
  void testWithFutureGroupMultiple() throws Exception {
    test(
        "select progressive * from t with future group by a, b",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        SqlNodeList.of(SqlUtils.getIdentifier("a"), SqlUtils.getIdentifier("b")));
  }
}
