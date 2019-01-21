package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.model.Partition;

public interface ProgressiveListener {
	void handle(Partition partition);
}
