package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.SQLiteDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlParserImpl;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseContextFactoryTest {

  private static Connection connection;

  private static Factory factory;

  private static SqlParser.Config config =
      SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).build();

  @BeforeAll
  static void beforeAll() throws SQLException {
    connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    factory = new Factory(SQLiteDriver.INSTANCE);
    config = SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).build();

    try (Statement statement = connection.createStatement()) {}
  }

  @AfterAll
  static void afterAll() {
    SqlUtils.closeSafe(connection);
  }

  private void test(String futureWhere, String columns, String sourceWhere) throws Exception {
    test(false, futureWhere, columns, sourceWhere, null);
  }

  private void test(
      boolean aggregation, String futureWhere, String columns, String sourceWhere, String groups)
      throws Exception {
    SqlCreateProgressiveView view =
        (SqlCreateProgressiveView)
            SqlParser.create(
                    String.format(
                        "create progressive view pv as select %s from v where %s",
                        aggregation ? "sum(z)" : "z", futureWhere),
                    config)
                .parseStmt();

    JdbcSourceContext context = factory.create(connection, view, null);

    SqlSelect select =
        (SqlSelect)
            SqlParser.create(
                    String.format(
                        "select %s %s from v %s %s",
                        aggregation ? "sum(z)" : "z",
                        columns == null ? "" : ", " + columns,
                        sourceWhere == null ? "" : "where " + sourceWhere,
                        groups == null ? "" : "group by " + groups))
                .parseQuery();

    assertTrue(select.equalsDeep(context.getSelectSource(), Litmus.THROW));
  }

  @Test
  void testFutureWhereOne() throws Exception {
    test("(a = 1) future", "a", null);
  }

  @Test
  void testFutureWhereTwoAnd() throws Exception {
    test("(a = 1) future and (b = 1) future", "a, b", null);
  }

  @Test
  void testFutureWhereTwoOr() throws Exception {
    test("(a = 1) future or (b = 1) future", "a, b", null);
  }

  @Test
  void testFutureWhereOneMixedAnd() throws Exception {
    test("(a = 1) future and b = 1", "a", "b = 1");
  }

  @Test
  void testFutureWhereOneMixedOr() throws Exception {
    test("(a = 1) future or b = 1", "a, b", "a = 1 or b = 1");
  }

  @Test
  void testFutureWhereTwoMixed() throws Exception {
    test("((a = 1) future or (a = 2) future) and b = 1", "a", "b = 1");
  }

  @Test
  void testFutureWhereTwoMixedReverse() throws Exception {
    test("((a = 1) future and (a = 2) future) or b = 1", "a, b", "a = 1 or a = 2 or b = 1");
  }

  @Test
  void testFutureWhereOneMultiple() throws Exception {
    test("(a = 1) future and (a = 2) future", "a", null);
  }

  @Test
  void testFutureWhereTwoMultiple() throws Exception {
    test("((a = 1) future or (a = 2) future) and ((b = 1) future or (b = 2) future)", "a, b", null);
  }

  @Test
  void testFutureWhereMultipleMixed() throws Exception {
    test("(a = 1) future and b = 1 and (c = 1) future", "a, c", "b = 1");
  }

  @Test
  void testFutureWhereTwoMultipleMixed() throws Exception {
    test(
        "((a = 1) future or (a = 2) future) and b = 1 and ((c = 1) future or (c = 2) future)",
        "a, c",
        "b = 1");
  }

  @Test
  void testFutureWhereOrMixed() throws Exception {
    test("((a = 1) future or (a = 2)) and b = 1", "a", "(a = 1 or a = 2) and b = 1");
  }

  @Test
  void testFutureWhereDeepOrMixed() throws Exception {
    test(
        "((a = 1) future or (a = 2) future or (a = 3)) and b = 1",
        "a",
        "(a = 1 or a = 2 or a = 3) and b = 1");
  }

  @Test
  void testFutureWhereAggregation() throws Exception {
    test(true, "(a = 1) future", "a", null, "a");
  }

  @Test
  void testFutureWhereNested() {
    assertThrows(
        IllegalArgumentException.class,
        () -> test("((a = 1) future or (a = 2)) future", "a", null));
  }

  static class Factory
      extends BaseContextFactory<JdbcSourceContext, JdbcSourceContext, JdbcDataBuffer> {

    public Factory(DbDriver sourceDriver) {
      super(sourceDriver);
    }

    @Override
    protected JdbcSourceContext create(
        Connection connection,
        SqlSelectProgressive select,
        Function<Pair<String, String>, Column> columnMapper,
        List<MetaField> metaFields,
        SqlSelect selectSource) {
      return null;
    }

    @Override
    protected JdbcSourceContext create(
        Connection connection,
        SqlCreateProgressiveView view,
        Function<Pair<String, String>, Column> columnMapper,
        List<MetaField> metaFields,
        SqlSelect selectSource) {
      return new JdbcSourceContext(metaFields, null, selectSource);
    }

    @Override
    public JdbcSourceContext create(
        JdbcDataBuffer dataBuffer,
        SqlSelectProgressive select,
        Function<Pair<String, String>, Column> columnMapper) {
      return null;
    }
  }
}
