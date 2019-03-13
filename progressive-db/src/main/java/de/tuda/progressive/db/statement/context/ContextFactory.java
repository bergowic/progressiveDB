package de.tuda.progressive.db.statement.context;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.statement.context.impl.BaseContext;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;

public interface ContextFactory<
    C1 extends JdbcSourceContext, C2 extends BaseContext, D extends DataBuffer> {

  C1 create(Connection connection, SqlSelect select);

  C2 create(D dataBuffer, SqlSelect select);

  C1 create(Connection connection, SqlCreateProgressiveView view);
}
