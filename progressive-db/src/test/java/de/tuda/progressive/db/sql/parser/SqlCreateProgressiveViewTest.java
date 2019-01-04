package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlCreateProgressiveViewTest {

	private SqlParser.ConfigBuilder configBuilder;

	@BeforeEach
	void init() {
		configBuilder = SqlParser.configBuilder()
				.setUnquotedCasing(Casing.UNCHANGED)
				.setParserFactory(SqlParserImpl.FACTORY);
	}

	@Test
	void valid() throws Exception {
		final String name = "a";
		final String table = "b";

		SqlParser parser = SqlParser.create(String.format("create progressive view %s as select * from %s", name, table), configBuilder.build());
		SqlNode node = parser.parseStmt();

		assertEquals(SqlCreateProgressiveView.class, node.getClass());

		SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

		assertEquals(name, createProgressiveView.getName().getSimple());
		assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

		SqlSelect select = (SqlSelect) createProgressiveView.getQuery();

		assertEquals(SqlIdentifier.class, select.getFrom().getClass());

		SqlIdentifier from = (SqlIdentifier) select.getFrom();

		assertEquals(table, from.getSimple());
	}

	private void testFutureGroupBy(Pair<String, Boolean>... columns) throws Exception {
		final String groupBy = String.join(", ", Arrays.stream(columns).map(p -> {
			String result = p.getLeft();
			if (p.getRight()) {
				result += " future";
			}
			return result;
		}).collect(Collectors.toList()));

		final String name = "a";
		final String table = "b";

		SqlParser parser = SqlParser.create(
				String.format("create progressive view %s as select * from %s group by %s", name, table, groupBy),
				configBuilder.build()
		);
		SqlNode node = parser.parseStmt();

		assertEquals(SqlCreateProgressiveView.class, node.getClass());

		SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

		assertEquals(name, createProgressiveView.getName().getSimple());
		assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

		SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
		SqlNodeList groupByList = select.getGroup();

		assertEquals(columns.length, groupByList.size());
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].getRight()) {
				assertEquals(SqlFutureIdentifier.class, groupByList.get(i).getClass());
			} else {
				assertEquals(SqlIdentifier.class, groupByList.get(i).getClass());
			}
			assertEquals(columns[i].getLeft(), ((SqlIdentifier) groupByList.get(i)).getSimple());
		}
	}

	@Test
	void groupBy() throws Exception {
		testFutureGroupBy(new ImmutablePair<>("a", false));
	}

	@Test
	void groupByFuture() throws Exception {
		testFutureGroupBy(new ImmutablePair<>("a", true));
	}

	@Test
	void groupByMultiple() throws Exception {
		testFutureGroupBy(
				new ImmutablePair<>("a", false),
				new ImmutablePair<>("b", false)
		);
	}

	@Test
	void groupByMultipleMixed() throws Exception {
		testFutureGroupBy(
				new ImmutablePair<>("a", false),
				new ImmutablePair<>("b", true)
		);
	}

	@Test
	void groupByMultipleFuture() throws Exception {
		testFutureGroupBy(
				new ImmutablePair<>("a", true),
				new ImmutablePair<>("b", true)
		);
	}
}