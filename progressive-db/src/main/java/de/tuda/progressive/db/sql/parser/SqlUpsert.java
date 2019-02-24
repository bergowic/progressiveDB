package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

public class SqlUpsert extends SqlInsert {

	private final SqlNodeList indexColumns;

	private final SqlUpdate update;

	public SqlUpsert(
			SqlParserPos pos,
			SqlNode targetTable,
			SqlNodeList columnList,
			SqlNode[] values,
			SqlNodeList indexColumns,
			SqlUpdate update
	) {
		super(
				pos,
				SqlNodeList.EMPTY,
				targetTable,
				new SqlBasicCall(
						SqlStdOperatorTable.VALUES,
						new SqlNode[]{new SqlBasicCall(
								new SqlRowOperator(" "),
								values,
								SqlParserPos.ZERO
						)},
						SqlParserPos.ZERO
				),
				columnList
		);

		this.indexColumns = indexColumns;
		this.update = update;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);

		final int opLeft = getOperator().getLeftPrec();
		final int opRight = getOperator().getRightPrec();

		writer.newlineAndIndent();
		writer.print("ON CONFLICT");
		writer.setNeedWhitespace(true);
		indexColumns.unparse(writer, opLeft, opRight);

		writer.newlineAndIndent();
		writer.print("DO");
		writer.setNeedWhitespace(true);

		update.unparse(writer, leftPrec, rightPrec);
	}
}
