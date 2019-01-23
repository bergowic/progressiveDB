package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureIdentifier;
import de.tuda.progressive.db.statement.context.Aggregation;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.SimpleStatementContext;
import de.tuda.progressive.db.statement.context.StatementContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ContextFactory {

	private static final Logger log = LoggerFactory.getLogger(ContextFactory.class);

	public static final ContextFactory instance = new ContextFactory();

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition) {
		final String cacheTableName = generateCacheTableName();
		return create(connection, driver, select, partition, cacheTableName);
	}

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition, String cacheTableName) {
		try {
			final List<SqlIdentifier> columnAliases = getColumnAliases(select.getSelectList());
			final List<Aggregation> aggregations = getAggregations(select.getSelectList());
			final SqlSelect sourceSelect = transformSelect(driver, select, aggregations);
			final PreparedStatement preparedStatement = prepareSourceSelect(connection, driver, sourceSelect);
			final ResultSetMetaData metaData = preparedStatement.getMetaData();
			final SqlCreateTable createCache = SqlUtils.createTable(driver, cacheTableName, metaData);
			final SqlInsert insertCache = insertCache(cacheTableName, metaData);
			final Pair<SqlSelect, Map<MetaField, Integer>> selectCache = selectCache(cacheTableName, metaData, columnAliases, aggregations, sourceSelect.getGroup());

			log.info("select source: {}", sourceSelect);

			return new StatementContext(
					sourceSelect,
					createCache,
					insertCache,
					selectCache.getLeft(),
					aggregations,
					selectCache.getRight()
			);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public SimpleStatementContext create(SimpleStatementContext context, SqlSelect select) {
		final SqlSelect viewSelect = new SqlSelect(
				SqlParserPos.ZERO,
				null,
				select.getSelectList(),
				context.getSelectCache(),
				select.getWhere(),
				select.getGroup(),
				select.getHaving(),
				select.getWindowList(),
				select.getOrderList(),
				select.getOffset(),
				select.getFetch()
		);

		return new SimpleStatementContext(viewSelect, context.getAggregations(), context.getMetaFieldPositions());
	}

	public StatementContext create(Connection connection, DbDriver driver, SqlCreateProgressiveView createProgressiveView) {
		final SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
		final String viewName = createProgressiveView.getName().getSimple();

		try {
			final List<SqlIdentifier> columnAliases = getColumnAliases(select.getSelectList());
			final List<Aggregation> aggregations = getAggregations(select.getSelectList());
			final SqlSelect sourceSelect = transformSelect(driver, select, aggregations);
			final int[] sourceSelectMapping = sortSelectList(driver, sourceSelect);
			final PreparedStatement preparedStatement = prepareSourceSelect(connection, driver, sourceSelect);
			final ResultSetMetaData metaData = preparedStatement.getMetaData();
			final SqlCreateTable createCache = SqlUtils.createTable(driver, viewName, metaData);
			final SqlInsert insertCache = insertCache(viewName, metaData);
			final Pair<SqlSelect, Map<MetaField, Integer>> selectCache = selectCache(viewName, metaData, sourceSelectMapping, columnAliases, aggregations, removeFutures(select.getGroup()));

			return new StatementContext(
					sourceSelect,
					createCache,
					insertCache,
					selectCache.getLeft(),
					aggregations,
					selectCache.getRight()
			);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	protected String generateCacheTableName() {
		return "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
	}

	private PreparedStatement prepareSourceSelect(Connection connection, DbDriver driver, SqlSelect select) {
		final String sql = driver.toSql(select);
		try {
			return connection.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private SqlSelect transformSelect(DbDriver driver, SqlSelect select, List<Aggregation> aggregations) {
		final SqlNodeList oldSelectList = select.getSelectList();
		final SqlNodeList oldGroups = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();
		SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		SqlNodeList groups = new SqlNodeList(SqlParserPos.ZERO);

		for (int i = 0; i < oldSelectList.size(); i++) {
			if (aggregations.get(i) == Aggregation.AVG) {
				SqlBasicCall call = (SqlBasicCall) oldSelectList.get(i);
				SqlBasicCall avg;
				if (call.getKind() == SqlKind.AS) {
					avg = (SqlBasicCall) call.getOperands()[0];
				} else {
					avg = call;
				}

				selectList.add(createSumAggregation(avg.getOperands()));
				selectList.add(createCountAggregation(avg.getOperands()));
			} else {
				selectList.add(oldSelectList.get(i));
			}
		}

		for (SqlNode group : oldGroups) {
			if (group instanceof SqlFutureIdentifier) {
				final SqlIdentifier name = new SqlIdentifier(((SqlFutureIdentifier) group).names, SqlParserPos.ZERO);
				selectList.add(name);
				groups.add(name);
			} else {
				groups.add(group);
			}
		}

		selectList.add(new SqlBasicCall(
				new SqlAsOperator(),
				new SqlNode[]{
						SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
						new SqlIdentifier("PARTITION", SqlParserPos.ZERO)},
				SqlParserPos.ZERO
		));

		final SqlIdentifier oldFrom = (SqlIdentifier) select.getFrom();
		SqlIdentifier from = new SqlIdentifier(Collections.singletonList(driver.getPartitionTable(oldFrom.getSimple())), SqlParserPos.ZERO);
		SqlBasicCall where = createWhere((SqlBasicCall) select.getWhere());

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
				select.getFetch()
		);
	}

	private int[] sortSelectList(DbDriver driver, SqlSelect select) {
		final SqlNodeList selectList = select.getSelectList();
		final List<String> selectStrings = get(selectList, driver::toSql);

		int[] permutation = IntStream.range(0, selectList.size()).toArray();
		for (int i = 0; i < selectList.size() - 1; i++) {
			int index = i;
			for (int j = i + 1; j < selectList.size() - 1; j++) {
				if (selectStrings.get(permutation[j]).compareTo(selectStrings.get(permutation[index])) < 0) {
					index = j;
				}
			}
			{
				SqlNode tmp = selectList.get(i);
				selectList.set(i, selectList.get(index));
				selectList.set(index, tmp);
			}
			{
				int tmp = permutation[i];
				permutation[i] = permutation[index];
				permutation[index] = tmp;
			}
		}

		int[] mapping = new int[permutation.length];
		for (int i = 0; i < mapping.length; i++) {
			mapping[permutation[i]] = i;
		}

		return mapping;
	}

	private SqlBasicCall createWhere(SqlBasicCall oldWhere) {
		final SqlBasicCall eqPartition = createWhereEqPartition();
		if (oldWhere == null) {
			return eqPartition;
		}

		return new SqlBasicCall(
				SqlStdOperatorTable.AND,
				new SqlNode[]{
						oldWhere,
						eqPartition
				},
				SqlParserPos.ZERO
		);
	}

	private SqlBasicCall createWhereEqPartition() {
		return new SqlBasicCall(
				SqlStdOperatorTable.EQUALS,
				new SqlNode[]{
						new SqlIdentifier(Collections.singletonList("_partition"), SqlParserPos.ZERO),
						new SqlDynamicParam(0, SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO
		);
	}

	private <T> List<T> get(SqlNodeList columns, Function<SqlNode, T> func) {
		return StreamSupport.stream(columns.spliterator(), false)
				.map(func)
				.collect(Collectors.toList());
	}

	private SqlNodeList removeFutures(SqlNodeList groups) {
		return new SqlNodeList(StreamSupport.stream(groups.spliterator(), false)
				.filter(n -> !(n instanceof SqlFutureIdentifier))
				.collect(Collectors.toList())
				, SqlParserPos.ZERO
		);
	}

	private List<SqlIdentifier> getColumnAliases(SqlNodeList columns) {
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

	private List<Aggregation> getAggregations(SqlNodeList columns) {
		return get(columns, this::columnToAggregation);
	}

	private Aggregation columnToAggregation(SqlNode column) {
		if (column instanceof SqlIdentifier || column instanceof SqlLiteral) {
			return Aggregation.NONE;
		}
		if (column instanceof SqlBasicCall) {
			SqlBasicCall call = (SqlBasicCall) column;
			SqlOperator operator = call.getOperator();
			switch (operator.getKind()) {
				case AVG:
					return Aggregation.AVG;
				case COUNT:
					return Aggregation.COUNT;
				case SUM:
					return Aggregation.SUM;
				case AS:
					return columnToAggregation(call.getOperands()[0]);
			}

			throw new IllegalArgumentException("operation is not supported: " + operator.getKind());
		}

		throw new IllegalArgumentException("column type is not supported: " + column.getClass());
	}

	private SqlInsert insertCache(String cacheTableName, ResultSetMetaData metaData) throws SQLException {
		final int count = metaData.getColumnCount();
		final SqlNode[] values = new SqlNode[count];

		for (int i = 1; i <= count; i++) {
			values[i - 1] = new SqlDynamicParam(i - 1, SqlParserPos.ZERO);
		}

		return new SqlInsert(
				SqlParserPos.ZERO,
				new SqlNodeList(SqlParserPos.ZERO),
				new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
				new SqlBasicCall(
						new SqlValuesOperator(),
						new SqlNode[]{
								new SqlBasicCall(
										new SqlRowOperator(" "),
										values,
										SqlParserPos.ZERO
								)
						},
						SqlParserPos.ZERO
				),
				null
		);
	}

	private Pair<SqlSelect, Map<MetaField, Integer>> selectCache(
			String cacheTableName,
			ResultSetMetaData metaData,
			List<SqlIdentifier> columnAliases,
			List<Aggregation> aggregations,
			SqlNodeList groups
	) {
		try {
			int[] selectMapping = IntStream.range(0, metaData.getColumnCount()).toArray();
			return selectCache(cacheTableName, metaData, selectMapping, columnAliases, aggregations, groups);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private Pair<SqlSelect, Map<MetaField, Integer>> selectCache(
			String cacheTableName,
			ResultSetMetaData metaData,
			int[] selectMapping,
			List<SqlIdentifier> columnAliases,
			List<Aggregation> aggregations,
			SqlNodeList groups
	) {
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		final Map<MetaField, Integer> metaFieldPositions = new HashMap<>();

		int i = 0;
		int index = 0;
		try {
			for (int j = 0; j < columnAliases.size(); j++) {
				final SqlIdentifier alias = columnAliases.get(j);
				final Aggregation aggregation = aggregations.get(j);

				final SqlIdentifier column = new SqlIdentifier(metaData.getColumnName(selectMapping[i] + 1), SqlParserPos.ZERO);
				SqlNode newColumn = null;

				switch (aggregation) {
					case NONE:
						newColumn = column;
						++i;
						break;
					case AVG:
						final SqlIdentifier nextColumn = new SqlIdentifier(metaData.getColumnName(selectMapping[i + 1] + 1), SqlParserPos.ZERO);
						newColumn = creatAvgAggregation(column, nextColumn);
						i += 2;
						break;
					case COUNT:
						newColumn = createCountPercentAggregation(index, column);
						++i;
						++index;
						break;
					case SUM:
						newColumn = createSumPercentAggregation(index, column);
						++i;
						++index;
						break;
				}

				selectList.add(alias == null ? newColumn : new SqlBasicCall(
						SqlStdOperatorTable.AS,
						new SqlNode[]{newColumn, alias},
						SqlParserPos.ZERO
				));
			}
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		addMetaField(selectList, index, "partition", SqlTypeName.INTEGER);
		metaFieldPositions.put(MetaField.PARTITION, index);

		++index;

		addMetaField(selectList, index, "progress", SqlTypeName.FLOAT);
		metaFieldPositions.put(MetaField.PROGRESS, index);

		return new ImmutablePair<>(
				new SqlSelect(
						SqlParserPos.ZERO,
						null,
						selectList,
						new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
						null,
						groups,
						null,
						null,
						null,
						null,
						null
				),
				metaFieldPositions
		);
	}

	private void addMetaField(SqlNodeList selectList, int index, String name, SqlTypeName typeName) {
		selectList.add(new SqlBasicCall(
				SqlStdOperatorTable.AS,
				new SqlNode[]{
						createCast(new SqlDynamicParam(index, SqlParserPos.ZERO), typeName),
						new SqlIdentifier(name, SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO
		));
	}

	private static SqlBasicCall createCast(SqlNode node, SqlTypeName typeName) {
		return new SqlBasicCall(
				SqlStdOperatorTable.CAST,
				new SqlNode[]{
						node,
						SqlUtils.getDataType(typeName)
				},
				SqlParserPos.ZERO
		);
	}

	private static SqlBasicCall creatAvgAggregation(SqlNode operand1, SqlNode operand2) {
		return new SqlBasicCall(
				SqlStdOperatorTable.DIVIDE,
				new SqlNode[]{
						createCast(createSumAggregation(operand1), SqlTypeName.FLOAT),
						createCast(createSumAggregation(operand2), SqlTypeName.FLOAT)
				},
				SqlParserPos.ZERO);
	}

	private static SqlBasicCall createCountAggregation(SqlNode... operands) {
		return new SqlBasicCall(
				new SqlCountAggFunction("COUNT"),
				operands,
				SqlParserPos.ZERO
		);
	}

	private static SqlBasicCall createCountPercentAggregation(int index, SqlNode operand) {
		return createPercentAggregation(index, createSumAggregation(operand));
	}

	private static SqlBasicCall createSumAggregation(SqlNode... operands) {
		return new SqlBasicCall(
				new SqlSumAggFunction(null),
				operands,
				SqlParserPos.ZERO
		);
	}

	private static SqlBasicCall createSumPercentAggregation(int index, SqlNode operand) {
		return createPercentAggregation(index, createSumAggregation(operand));
	}

	private static SqlBasicCall createPercentAggregation(int index, SqlBasicCall aggregation) {
		return new SqlBasicCall(
				SqlStdOperatorTable.DIVIDE,
				new SqlNode[]{
						aggregation,
						new SqlDynamicParam(index, SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO);
	}
}
