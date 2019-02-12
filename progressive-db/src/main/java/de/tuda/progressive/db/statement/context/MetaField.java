package de.tuda.progressive.db.statement.context;

public enum MetaField {
	NONE(false, false),
	AVG(false, false),
	COUNT(false, true),
	SUM(false, true),
	PARTITION(true, true),
	PROGRESS(true, true);

	private final boolean function;

	private final boolean substitute;

	MetaField(boolean function, boolean substitute) {
		this.function = function;
		this.substitute = substitute;
	}

	public boolean isFunction() {
		return function;
	}

	public boolean isSubstitute() {
		return substitute;
	}
}
