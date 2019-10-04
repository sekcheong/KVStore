package com.cs739.kvstore;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.cs739.kvstore.datastore.DataStore;
import com.cs739.kvstore.datastore.DataStoreFactory;
import com.cs739.kvstore.datastore.PutValueRequest;
import com.cs739.kvstore.datastore.PutValueResponse;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ClientRequestHandlerThread implements Runnable {
	private Socket socket;
	private int externalPort;
	private List<Integer> servers;
	private CopyOnWriteArrayList<Boolean> serverStatus;
	private BlockingQueue<String> blockingQueue;
	private DataStore dataStore;
	public ClientRequestHandlerThread(Socket socket,
			BlockingQueue<String> blockingQueue, List<Integer> servers, int externalPort, CopyOnWriteArrayList<Boolean> serverStatus) {
		this.socket = socket;
		this.dataStore = DataStoreFactory.getDataStore();
		this.blockingQueue = blockingQueue;
		this.servers = servers;
		this.externalPort = externalPort;
		this.serverStatus = serverStatus;
	}

	public int hashFunc(String key) {
		System.out.println();
		int primary = key.hashCode() % servers.size();
		if(primary < 0) {
			primary = primary*-1;
		}
		while (!serverStatus.get(primary)) {
			primary = (primary + 1) % servers.size();
		}
		return primary;
	}
	@Override
	public void run() {
		System.out.println("Accepted connection from client");
		Scanner in = null;
		try {
			in = new Scanner(socket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter out = null;
		try {
			out = new PrintWriter(socket.getOutputStream(),
					true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		while (in.hasNextLine()) {
			String request = in.nextLine();
			System.out.println("Client Request: " + request);
			JsonObject jsonObject = new JsonParser().parse(request).getAsJsonObject();
			String operation = jsonObject.get("operation").getAsString();
			JsonObject response = new JsonObject();
			if (operation.equals("GET")) {
				String key = jsonObject.get("key").getAsString();
				try {
					String value = dataStore.getValue(key);
					response.addProperty("status", "success");
					response.addProperty("value", value);
				} catch (SQLException e) {
					response.addProperty("status", "failure");
					e.printStackTrace();
				}
				out.println(response.toString());
			} else if (operation.equals("PUT")) {
				// TODO: Put hash logic
				String key = jsonObject.get("key").getAsString();
				String value = jsonObject.get("value").getAsString();
				int primary = hashFunc(key);
				// This is the primary
				if (servers.get(primary) == externalPort) {
					try {
						PutValueResponse putValueResponse = dataStore.putValue(key, value, PutValueRequest.APPLY_PRIMARY_UPDATE,
								-1);
						response.addProperty("status", "success");
						response.addProperty("value", putValueResponse.getOldValue());
						jsonObject.addProperty("seq", putValueResponse.getSequenceNumber());
						// Broadcast to other servers
						blockingQueue.add(jsonObject.toString());
					} catch (SQLException e) {
						response.addProperty("status", "failure");
						e.printStackTrace();
					}
					out.println(response.toString());
				} else {
					try {
					PutValueResponse putValueResponse = dataStore.putValue(key, value, PutValueRequest.APPLY_FOLLOWER_UPDATE,
							-1);
					response.addProperty("status", "success");
					response.addProperty("value", putValueResponse.getOldValue());
					out.println(response.toString());
					boolean forwarded = false;
					while (!forwarded) {
						System.out.println("Primary server is " + servers.get(primary));
						// Some other server is primary
						Socket primarySocket = null;
						PrintWriter primaryWriter = null;
						try {
							primarySocket = new Socket("127.0.0.1", servers.get(primary));
							primaryWriter = new PrintWriter(primarySocket.getOutputStream(), true);
							primaryWriter.println(jsonObject.toString());
							forwarded = true;
						} catch (IOException e) {
							// TODO: Should we check for primarySocket NULL?
							// Set 'primary' server status as dead
							serverStatus.set(primary, false);
							// Broadcast to other servers that 'primary' is dead
							JsonObject serverDeadMessage = new JsonObject();
							serverDeadMessage.addProperty("operation", "SERVER_DOWN");
							serverDeadMessage.addProperty("server", primary);
							blockingQueue.add(serverDeadMessage.toString());
							// Find new 'primary' to forward the message to
							primary = hashFunc(key);
							e.printStackTrace();
						}
					}
					} catch (SQLException e) {
						response.addProperty("status", "failure");
						out.println(response.toString());
						e.printStackTrace();
					}
				}
			} else if (operation.equals("GET_SEQ_NO")) {
				String key = jsonObject.get("key").getAsString();
				JsonObject jsonobject = new JsonObject();
				jsonobject.addProperty("seq", dataStore.getSequenceNumber(key));
				out.println(jsonobject.toString());
			}
		}
		in.close();
		out.close();	
	}

}
