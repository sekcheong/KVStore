package com.cs739.kvstore;

import java.io.IOException;
import java.io.File;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.cs739.kvstore.MulticastReceiverThread;
import com.cs739.kvstore.MulticastSenderThread;
import com.cs739.kvstore.datastore.DataStore;
import com.cs739.kvstore.datastore.DataStoreFactory;
import com.google.gson.JsonObject;
import com.cs739.kvstore.ClientRequestHandlerThread;

public class KeyValueServer {
	Socket externalSocket;
	int externalPort;
	InetAddress broadcastIP;
	DatagramSocket datagramSocket;
	MulticastSocket multicastSocket;
	BlockingQueue<String> blockingQueue;
	DataStore dataStore;
	List<Integer> servers;
	CopyOnWriteArrayList<Boolean> serverStatus;
	int datagramPort;
	int internalPort;
	List<Integer> internalPorts;
	ServerSocket internalSocket;

	public KeyValueServer(int externalPort, 
			InetAddress broadcastIP, List<Integer> servers, int datagramPort, 
			CopyOnWriteArrayList<Boolean> serverStatus, int internalPort, List<Integer> internalPorts) {
		this.externalPort = externalPort;
		this.broadcastIP = broadcastIP;
		this.blockingQueue = new LinkedBlockingQueue<>();
		this.servers = servers;
		this.datagramPort = datagramPort;
		this.serverStatus = serverStatus;
		this.internalPort = internalPort;
		this.internalPorts = internalPorts;
		this.dataStore = DataStoreFactory.createDataStore(externalPort, this.servers, this.serverStatus, this.blockingQueue,
				this.internalPorts);
		try {
			this.datagramSocket = new DatagramSocket(datagramPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			this.multicastSocket = new MulticastSocket(4446);
			this.multicastSocket.joinGroup(this.broadcastIP);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			this.internalSocket = new ServerSocket(internalPort,
					0, InetAddress.getByName("127.0.0.1"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		File file = new File(args[1]);
		Scanner sc = new Scanner(file);
		int externalPort = Integer.parseInt(args[0]);
		List<Integer> servers = new ArrayList<Integer>();
		CopyOnWriteArrayList<Boolean> serverStatus = new CopyOnWriteArrayList<Boolean>();
		int datagramPort = -1;
		int internalPort = -1;
		List<Integer> internalPorts = new ArrayList<Integer>();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			int serverPort = Integer.parseInt(currentLine.split(",")[0]);
			int currentdatagramPort = Integer.parseInt(currentLine.split(",")[1]);
			int currentInternalPort = Integer.parseInt(currentLine.split(",")[2]);
			if (serverPort == externalPort) {
				datagramPort = currentdatagramPort;
				internalPort = currentInternalPort;
			}
			servers.add(serverPort);
			internalPorts.add(internalPort);
			serverStatus.add(true);
		}
		sc.close();
		InetAddress broadcastIP = InetAddress.getByName("224.0.113.0");
		KeyValueServer keyValueServer = new KeyValueServer(externalPort,
				broadcastIP, servers, datagramPort, serverStatus, internalPort, internalPorts);
		keyValueServer.start();
	}
	
	public void start() {
		Thread t1 = new Thread (new MulticastReceiverThread(getMulticastSocket(), serverStatus));
		t1.start();
		Thread t2 = new Thread(new MulticastSenderThread(getDatagramSocket(),
				getBroadcastIP(), getBlockingQueue()));
		t2.start();
		Thread t3 = new Thread (new InternalPortThread(internalSocket));
		t3.start();
		// Broadcast to all servers that the server started
		JsonObject serverAliveMessage = new JsonObject();
		serverAliveMessage.addProperty("operation", "SERVER_UP");
		serverAliveMessage.addProperty("server", servers.indexOf(externalPort));
		blockingQueue.add(serverAliveMessage.toString());
		try (ServerSocket listener = new ServerSocket(externalPort,
				0, InetAddress.getByName("127.0.0.1"))) {
			System.out.println("The key value server is running at localhost...");
			ExecutorService pool = Executors.newFixedThreadPool(20);
			while (true) {
				pool.execute(new ClientRequestHandlerThread(listener.accept(), 
						getBlockingQueue(), servers, externalPort, serverStatus));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		return "Client facing port: " + externalPort;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public InetAddress getBroadcastIP() {
		return broadcastIP;
	}

	public MulticastSocket getMulticastSocket() {
		return multicastSocket;
	}

	public DatagramSocket getDatagramSocket() {
		return datagramSocket;
	}

	public BlockingQueue<String> getBlockingQueue() {
		return blockingQueue;
	}
}
