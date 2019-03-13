package de.tuda.progressive.db.buffer;

import de.tuda.progressive.db.statement.context.impl.BaseContext;

public interface DataBufferFactory<C extends BaseContext> {
  DataBuffer create(C context);
}
