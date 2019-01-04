package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlFutureIdentifier extends SqlIdentifier {

	public SqlFutureIdentifier(String name, SqlParserPos pos) {
		super(name, pos);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		writer.keyword("FUTURE");
	}
}
