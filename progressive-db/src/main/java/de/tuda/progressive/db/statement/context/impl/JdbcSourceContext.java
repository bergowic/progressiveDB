package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;

public abstract class JdbcSourceContext extends BaseContext {

  private final SqlSelect selectSource;

  public JdbcSourceContext(List<MetaField> metaFields, SqlSelect selectSource) {
    super(metaFields);

    this.selectSource = selectSource;
  }

  public SqlSelect getSelectSource() {
    return selectSource;
  }

  public abstract static class Builder<C extends JdbcSourceContext, B extends Builder<C, B>>
      extends BaseContext.Builder<C, B> {
    private SqlSelect selectSource;

    public B selectSource(SqlSelect selectBuffer) {
      this.selectSource = selectBuffer;
      return (B) this;
    }

    @Override
    protected final C build(List<MetaField> metaFields) {
      return build(metaFields, selectSource);
    }

    protected abstract C build(List<MetaField> metaFields, SqlSelect selectSource);
  }
}
