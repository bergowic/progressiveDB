package de.tuda.progressive.db.meta;

import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;

import java.util.List;

public interface MetaData {

	void add(List<Partition> partitions, List<Column> columns);

	List<Partition> getPartitions(String table);

	Column getColumn(String table, String column);
}
