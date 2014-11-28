package org.opencloudb.backend;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConQueue {
	private final ConcurrentLinkedQueue<BackendConnection> autoCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	private final ConcurrentLinkedQueue<BackendConnection> manCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	private long executeCount;

	public BackendConnection takeIdleCon(ConnectionMeta conMeta) {
		ConcurrentLinkedQueue<BackendConnection> f1 = autoCommitCons;
		ConcurrentLinkedQueue<BackendConnection> f2 = manCommitCons;

		if (!conMeta.isAutocommit()) {
			f1 = manCommitCons;
			f2 = autoCommitCons;

		}
		BackendConnection con = f1.poll();
		if (con == null) {
			con = f2.poll();
		}
		return con;

	}

	public long getExecuteCount() {
		return executeCount;
	}

	public void incExecuteCount() {
		this.executeCount++;
	}

	public boolean isSameCon(BackendConnection con) {
		if (autoCommitCons.contains(con)) {
			return true;
		} else if (manCommitCons.contains(con)) {
			return true;
		}
		return false;
	}

	public ConcurrentLinkedQueue<BackendConnection> getAutoCommitCons() {
		return autoCommitCons;
	}

	public ConcurrentLinkedQueue<BackendConnection> getManCommitCons() {
		return manCommitCons;
	}

	public ArrayList<BackendConnection> getIdleConsToClose(int count) {
		ArrayList<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(
				count);
		while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = manCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}
		while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = autoCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}

		}
		return readyCloseCons;
	}

}
