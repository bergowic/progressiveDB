package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcBufferContext extends JdbcSourceContext {

  private final List<String> fieldNames;

  private final SqlSelect selectBuffer;

  public JdbcBufferContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource,
      List<String> fieldNames,
      SqlSelect selectBuffer) {
    super(metaFields, bounds, selectSource);

    this.fieldNames = fieldNames;
    this.selectBuffer = selectBuffer;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public int getFieldIndex(String fieldName) {
    for (int i = 0; i < fieldNames.size(); i++) {
      if (fieldName.equalsIgnoreCase(fieldNames.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public SqlSelect getSelectBuffer() {
    return selectBuffer;
  }

  protected abstract static class AbstractBuilder<
          C extends JdbcBufferContext, B extends AbstractBuilder<C, B>>
      extends JdbcSourceContext.AbstractBuilder<C, B> {

    private List<String> fieldNames;

    private SqlSelect selectBuffer;

    public B fieldNames(List<String> fieldNames) {
      this.fieldNames = fieldNames;
      return (B) this;
    }

    public B selectBuffer(SqlSelect selectBuffer) {
      this.selectBuffer = selectBuffer;
      return (B) this;
    }

    @Override
    protected final C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource) {
      return build(metaFields, bounds, selectSource, fieldNames, selectBuffer);
    }

    protected abstract C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<String> fieldNames,
        SqlSelect selectBuffer);
  }

  public static class Builder extends AbstractBuilder<JdbcBufferContext, Builder> {
    @Override
    protected JdbcBufferContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<String> fieldNames,
        SqlSelect selectBuffer) {
      return new JdbcBufferContext(metaFields, bounds, selectSource, fieldNames, selectBuffer);
    }
  }
}
