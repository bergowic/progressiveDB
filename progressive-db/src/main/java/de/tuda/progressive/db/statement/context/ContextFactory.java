package de.tuda.progressive.db.statement.context;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class ContextFactory<C extends BaseContext> {

	protected final DbDriver sourceDriver;

	public ContextFactory(DbDriver sourceDriver) {
		this.sourceDriver = sourceDriver;
	}

	public C create(Connection connection, SqlSelect select) {
		final List<MetaField> metaFields = getMetaFields(select.getSelectList());
		final SqlSelect selectSource = transformSelect(select, metaFields);

		return create(
				connection,
				select,
				metaFields,
				selectSource
		);
	}

	protected abstract C create(
			Connection connection,
			SqlSelect select,
			List<MetaField> metaFields,
			SqlSelect selectSource
	);

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

	private List<MetaField> getMetaFields(SqlNodeList columns) {
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
			}

			throw new IllegalArgumentException("operation is not supported: " + operator.getName());
		}

		throw new IllegalArgumentException("column type is not supported: " + column.getClass());
	}

	private SqlSelect transformSelect(SqlSelect select, List<MetaField> metaFields) {
		final SqlNodeList oldSelectList = select.getSelectList();
		final SqlNodeList oldGroups = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();

		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		final SqlNodeList groups = new SqlNodeList(SqlParserPos.ZERO);

		for (int i = 0; i < oldSelectList.size(); i++) {
			switch (metaFields.get(i)) {
				case AVG:
					SqlBasicCall call = (SqlBasicCall) oldSelectList.get(i);
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
				default:
					throw new IllegalArgumentException("metaField not supported: " + metaFields.get(i));
			}
		}

		for (SqlNode group : oldGroups) {
			groups.add(group);
		}

		final SqlIdentifier oldFrom = (SqlIdentifier) select.getFrom();
		SqlIdentifier from = new SqlIdentifier(sourceDriver.getPartitionTable(oldFrom.getSimple()), SqlParserPos.ZERO);
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
}
