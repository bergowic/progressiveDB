package de.tuda.progressive.db;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemoryMetaData implements MetaData {

	private Map<String, List<Partition>> partitions = new HashMap<>();

	private Map<String, Map<String, Column>> columns = new HashMap<>();

	@Override
	public void add(String table, List<Partition> partitions, List<Column> columns) {
		this.partitions.put(table, partitions);
		this.columns.put(table, columns.stream().collect(Collectors.toMap(Column::getName, c -> c)));
	}

	@Override
	public List<Partition> getPartitions(String table) {
		return partitions.get(table);
	}

	@Override
	public Column getColumn(String table, String column) {
		return columns.getOrDefault(table, Collections.emptyMap()).get(column);
	}
}
