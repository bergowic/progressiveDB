package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class BaseContext {

  private final List<MetaField> metaFields;

  private final Map<Integer, Pair<Integer, Integer>> bounds;

  public BaseContext(List<MetaField> metaFields, Map<Integer, Pair<Integer, Integer>> bounds) {
    this.metaFields = metaFields;
    this.bounds = bounds;
  }

  public Optional<Integer> getFunctionMetaFieldPos(MetaField metaField, boolean substitute) {
    if (!metaField.isFunction()) {
      throw new IllegalArgumentException("metaField must be a function");
    }

    int pos = 0;
    for (MetaField m : metaFields) {
      if (m == metaField) {
        return Optional.of(pos);
      }

      if (!substitute || m.isSubstitute()) {
        pos++;
      }
    }

    return Optional.empty();
  }

  public List<MetaField> getMetaFields() {
    return metaFields;
  }

  public Map<Integer, Pair<Integer, Integer>> getBounds() {
    return bounds;
  }

  public Pair<Integer, Integer> getBound(int index) {
    return bounds.get(index);
  }

  @SuppressWarnings("unchecked")
  public abstract static class Builder<C extends BaseContext, B extends Builder<C, B>> {
    private List<MetaField> metaFields;

    private Map<Integer, Pair<Integer, Integer>> bounds;

    public B metaFields(List<MetaField> metaFields) {
      this.metaFields = metaFields;
      return (B) this;
    }

    public B bounds(Map<Integer, Pair<Integer, Integer>> bounds) {
      this.bounds = bounds;
      return (B) this;
    }

    public final C build() {
      return build(metaFields, bounds);
    }

    protected abstract C build(
        List<MetaField> metaFields, Map<Integer, Pair<Integer, Integer>> bounds);
  }
}
