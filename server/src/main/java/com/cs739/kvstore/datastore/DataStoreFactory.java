package com.cs739.kvstore.datastore;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataStoreFactory {

	static DataStore sInstance = null;

	public static DataStore createDataStore(int port, List<Integer> servers, CopyOnWriteArrayList<Boolean> serverStatus,
			BlockingQueue<String> blockingQueue, List<Integer> internalPorts) {
		sInstance = new SQLDataStore(port, servers, serverStatus, blockingQueue, internalPorts);
		return sInstance;
	}
	public static DataStore getDataStore() {
		return sInstance;
	}
}