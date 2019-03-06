package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;

public class SqlFutureNode extends SqlNodeList {

  public SqlFutureNode(SqlNode node, SqlParserPos pos) {
    super(Collections.singleton(node), pos);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, 0, 0);
    writer.keyword("FUTURE");
  }

  public SqlNode getNode() {
    return getList().get(0);
  }
}
