package de.tuda.progressive.db.model;

public class Partition {

	private String srcTable;

	private String tableName;

	private int id;

	private long entries;

	public Partition() {}

	public Partition(String srcTable, String tableName, int id, long entries) {
		this.srcTable = srcTable;
		this.tableName = tableName;
		this.id = id;
		this.entries = entries;
	}

	public String getSrcTable() {
		return srcTable;
	}

	public void setSrcTable(String srcTable) {
		this.srcTable = srcTable;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getEntries() {
		return entries;
	}

	public void setEntries(long entries) {
		this.entries = entries;
	}
}
