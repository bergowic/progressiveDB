package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcSelectContext extends JdbcBufferContext {

  private final SqlCreateTable createBuffer;

  private final SqlInsert insertBuffer;

  private final SqlUpdate updateBuffer;

  public JdbcSelectContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource,
      List<String> fieldNames,
      SqlCreateTable createBuffer,
      SqlInsert insertBuffer,
      SqlUpdate updateBuffer,
      SqlSelect selectBuffer) {
    super(metaFields, bounds, selectSource, fieldNames, selectBuffer);

    this.createBuffer = createBuffer;
    this.insertBuffer = insertBuffer;
    this.updateBuffer = updateBuffer;
  }

  public SqlCreateTable getCreateBuffer() {
    return createBuffer;
  }

  public SqlInsert getInsertBuffer() {
    return insertBuffer;
  }

  public SqlUpdate getUpdateBuffer() {
    return updateBuffer;
  }

  public static final class Builder
      extends JdbcBufferContext.AbstractBuilder<JdbcSelectContext, Builder> {
    private SqlCreateTable createBuffer;

    private SqlInsert insertBuffer;

    private SqlUpdate updateBuffer;

    public Builder createBuffer(SqlCreateTable createBuffer) {
      this.createBuffer = createBuffer;
      return this;
    }

    public Builder insertBuffer(SqlInsert insertBuffer) {
      this.insertBuffer = insertBuffer;
      return this;
    }

    public Builder updateBuffer(SqlUpdate updateBuffer) {
      this.updateBuffer = updateBuffer;
      return this;
    }

    @Override
    protected final JdbcSelectContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<String> fieldNames,
        SqlSelect selectBuffer) {
      return new JdbcSelectContext(
          metaFields,
          bounds,
          selectSource,
          fieldNames,
          createBuffer,
          insertBuffer,
          updateBuffer,
          selectBuffer);
    }
  }
}
