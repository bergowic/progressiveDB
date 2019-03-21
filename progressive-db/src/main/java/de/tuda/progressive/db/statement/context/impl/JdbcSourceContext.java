package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcSourceContext extends BaseContext {

  private final SqlSelect selectSource;

  public JdbcSourceContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource) {
    super(metaFields, bounds);

    this.selectSource = selectSource;
  }

  public SqlSelect getSelectSource() {
    return selectSource;
  }

  public abstract static class AbstractBuilder<
          C extends JdbcSourceContext, B extends AbstractBuilder<C, B>>
      extends BaseContext.Builder<C, B> {
    private SqlSelect selectSource;

    public B selectSource(SqlSelect selectBuffer) {
      this.selectSource = selectBuffer;
      return (B) this;
    }

    @Override
    protected final C build(
        List<MetaField> metaFields, Map<Integer, Pair<Integer, Integer>> bounds) {
      return build(metaFields, bounds, selectSource);
    }

    protected abstract C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource);
  }

  public static class Builder extends AbstractBuilder<JdbcSourceContext, Builder> {
    @Override
    protected JdbcSourceContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource) {
      return new JdbcSourceContext(metaFields, bounds, selectSource);
    }
  }
}
