package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.MetaField;

import java.util.List;
import java.util.Optional;

public abstract class BaseContext {

  private final List<MetaField> metaFields;

  public BaseContext(List<MetaField> metaFields) {
    this.metaFields = metaFields;
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

  @SuppressWarnings("unchecked")
  public abstract static class Builder<C extends BaseContext, B extends Builder<C, B>> {
    private List<MetaField> metaFields;

    public B metaFields(List<MetaField> metaFields) {
      this.metaFields = metaFields;
      return (B) this;
    }

    public final C build() {
      return build(metaFields);
    }

    protected abstract C build(List<MetaField> metaFields);
  }
}
