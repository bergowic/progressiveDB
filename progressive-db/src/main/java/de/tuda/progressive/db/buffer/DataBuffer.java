package de.tuda.progressive.db.buffer;

import java.sql.ResultSet;
import java.util.List;

public interface DataBuffer extends AutoCloseable {

	void add(ResultSet result);

	List<Object[]> get(int partition, double progress);
}
