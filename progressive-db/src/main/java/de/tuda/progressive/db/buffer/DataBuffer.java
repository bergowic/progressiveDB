package de.tuda.progressive.db.buffer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public interface DataBuffer<C> extends AutoCloseable {

  void add(ResultSet result);

  List<Object[]> get(int partition, double progress);

  ResultSetMetaData getMetaData();

  C getContext();
}
