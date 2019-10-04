package com.cs739.kvstore;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import com.cs739.kvstore.datastore.DataStore;
import com.cs739.kvstore.datastore.DataStoreFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InternalPortThread implements Runnable {
	ServerSocket socket;
	DataStore dataStore;
	public InternalPortThread(ServerSocket socket) {
		this.socket = socket;
		this.dataStore = DataStoreFactory.getDataStore();
	}
	@Override
	public void run() {
		while(true) {
			try {
				Socket requestSocket = socket.accept();
				Scanner in = new Scanner(requestSocket.getInputStream());
				PrintWriter out = new PrintWriter(requestSocket.getOutputStream(), true);
				while (in.hasNextLine()) {
					String request = in.nextLine();
					System.out.println("Internal Request: " + request);
					JsonObject jsonObject = new JsonParser().parse(request).getAsJsonObject();
					String operation = jsonObject.get("operation").getAsString();
					JsonObject response = new JsonObject();
					if (operation.equals("GET_SEQ_NO")) {
						String key = jsonObject.get("key").getAsString();
						response.addProperty("seq", dataStore.getSequenceNumber(key));
						out.println(response.toString());
					}
				}
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
