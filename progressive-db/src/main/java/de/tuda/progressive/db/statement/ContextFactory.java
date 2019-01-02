package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ContextFactory {

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
			final SqlSelect selectCache = selectCache(cacheTableName, metaData, columnAliases, aggregations, sourceSelect.getGroup());

			return new StatementContext(sourceSelect, createCache, insertCache, selectCache, aggregations);
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
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

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
				select.getGroup(),
				select.getHaving(),
				select.getWindowList(),
				select.getOrderList(),
				select.getOffset(),
				select.getFetch()
		);
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

	private SqlSelect selectCache(
			String cacheTableName,
			ResultSetMetaData metaData,
			List<SqlIdentifier> columnAliases,
			List<Aggregation> aggregations,
			SqlNodeList groups
	) {
		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);

		int i = 0;
		int index = 0;
		try {
			for (int j = 0; j < columnAliases.size(); j++) {
				final SqlIdentifier alias = columnAliases.get(j);
				final Aggregation aggregation = aggregations.get(j);

				final SqlIdentifier column = new SqlIdentifier(metaData.getColumnName(i + 1), SqlParserPos.ZERO);
				SqlNode newColumn = null;

				switch (aggregation) {
					case NONE:
						newColumn = column;
						++i;
						break;
					case AVG:
						final SqlIdentifier nextColumn = new SqlIdentifier(metaData.getColumnName(i + 2), SqlParserPos.ZERO);
						newColumn = creatAvgAggregation(column, nextColumn);
						i += 2;
						++index;
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

				columns.add(alias == null ? newColumn : new SqlBasicCall(
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

		columns.add(new SqlBasicCall(
				SqlStdOperatorTable.AS,
				new SqlNode[]{
						new SqlDynamicParam(index, SqlParserPos.ZERO),
						new SqlIdentifier("partition", SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO
		));

		return new SqlSelect(
				SqlParserPos.ZERO,
				null,
				columns,
				new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
				null,
				groups,
				null,
				null,
				null,
				null,
				null
		);
	}

	private static SqlBasicCall creatAvgAggregation(SqlNode operand1, SqlNode operand2) {
		return new SqlBasicCall(
				SqlStdOperatorTable.DIVIDE,
				new SqlNode[]{createSumAggregation(operand1), createSumAggregation(operand2)},
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
