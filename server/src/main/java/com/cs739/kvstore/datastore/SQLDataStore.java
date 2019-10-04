package com.cs739.kvstore.datastore;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SQLDataStore implements DataStore {

	String mDbName;
	Connection mDatabaseConnection;
	List<Integer> servers;
	List<Integer> internalPorts;
	CopyOnWriteArrayList<Boolean> serverStatus;
	BlockingQueue<String> blockingQueue;
	Integer port;

	public SQLDataStore(int port, List<Integer> servers, CopyOnWriteArrayList<Boolean> serverStatus,
			BlockingQueue<String> blockingQueue, List<Integer> internalPorts) {
		this.servers = servers;
		this.internalPorts = internalPorts;
		this.serverStatus = serverStatus;
		this.blockingQueue = blockingQueue;
		this.port = port;
		mDbName = "kvstore_" + Integer.toString(port) + ".db";
		establishConnection();
		try {
			createDataStoreIfNotExists();
			// Mark all entries all potentially stale
			markAllKeyStale();
			resendDirtyKeys();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// Resend dirty keys - could have been possible that we updated DB but did not broadcast
	}

	@Override
	public synchronized PutValueResponse putValue(String key, String value, PutValueRequest type, 
			int updateSequenceNumber) throws SQLException {
		// First, need to check old value
		String queryForPresenceOfKey = "SELECT * FROM kvstore_schema where key=\"" + key + "\"";
		ResultSet res = executeQuery(queryForPresenceOfKey);
		int size = 0;
		String oldValue = null;
		int seqToReturn = 0;
		Integer stored_seqno = 0;
		boolean might_be_stale = true;
		while(res.next()) {
			size++;
			oldValue = res.getString("value");
			stored_seqno = res.getInt("sequence_number");
			might_be_stale = res.getBoolean("might_be_stale");
		}
		System.out.println(might_be_stale);
		assert(size == 0 || size == 1);
		if(might_be_stale && type == PutValueRequest.APPLY_PRIMARY_UPDATE) {
			// Need to contact other servers for latest sequence number
			System.out.println("Sequence number for key might be stale");
			JsonObject seqRequest = new JsonObject();
			seqRequest.addProperty("operation", "GET_SEQ_NO");
			seqRequest.addProperty("key", key);
			for (int i = 0; i < serverStatus.size(); ++i) {
				if (serverStatus.get(i) && (servers.get(i) != port)) {
					System.out.println("Contacting server " + Integer.toString(servers.get(i)));
					Socket contactSocket = null;
					PrintWriter contactWriter = null;
					Scanner incoming = null;
					try {
						contactSocket = new Socket("127.0.0.1", internalPorts.get(i));
						contactWriter = new PrintWriter(contactSocket.getOutputStream(), true);
						incoming = new Scanner(contactSocket.getInputStream());
						contactWriter.println(seqRequest.toString());
						while(incoming.hasNextLine()) {
							String response = incoming.nextLine();
							System.out.println("Response from server " +
									Integer.toString(servers.get(i)) + " is " + response);
							JsonObject jsonObject = new JsonParser().parse(response).getAsJsonObject();
							Integer seq = jsonObject.get("seq").getAsInt();
							if(seq > stored_seqno) {
								stored_seqno = seq;
							}
							break;
						}
						contactSocket.close();
					} catch (Exception e) {
						serverStatus.set(i, false);
						JsonObject serverDeadMessage = new JsonObject();
						serverDeadMessage.addProperty("operation", "SERVER_DOWN");
						serverDeadMessage.addProperty("server", i);
						blockingQueue.add(serverDeadMessage.toString());
						e.printStackTrace();
					}
				}
			}
		}
		if(size == 0 ) {
			// need to insert value for key
			int seqno = 0;
			boolean isDirty = false;
			if (type == PutValueRequest.APPLY_FOLLOWER_PERSIST) {
				seqno = updateSequenceNumber;
				isDirty = false;
			} else if (type == PutValueRequest.APPLY_FOLLOWER_UPDATE) {
				seqno = stored_seqno;
				isDirty = true;
			} else if (type == PutValueRequest.APPLY_PRIMARY_UPDATE) {
				seqno = stored_seqno + 1;
				isDirty = false;
			}
			System.out.println(stored_seqno + ":" + seqno);
			if(seqno >= stored_seqno) {
				seqToReturn = seqno;
				String insertQuery = new StringBuilder("INSERT INTO kvstore_schema values(\"")
						.append(key)
						.append("\",\"")
						.append(value)
						.append("\",")
						.append(seqno)
						.append(",\"")
						.append(isDirty)
						.append("\",\"")
						.append("false")
						.append("\")").toString();
				System.out.println(insertQuery);
				executeUpdate(insertQuery);
			}
		} else {
			// need to update value for key
			int seqno = stored_seqno;
			boolean isDirty = false;
			if(type == PutValueRequest.APPLY_FOLLOWER_UPDATE) {
				seqno = stored_seqno;
				isDirty = true;
			} else if (type == PutValueRequest.APPLY_FOLLOWER_PERSIST) {
				seqno = updateSequenceNumber;
				isDirty = false;
			} else if (type == PutValueRequest.APPLY_PRIMARY_UPDATE) {
				seqno = stored_seqno + 1;
				isDirty = false;
			}
			if(seqno >= stored_seqno) {
				seqToReturn = seqno;
				String updateQuery = new StringBuilder("UPDATE kvstore_schema set value=\"")
						.append(value)
						.append("\",sequence_number=")
						.append(seqno)
						.append(",dirty=\"")
						.append(isDirty)
						.append("\",might_be_stale=\"")
						.append("false")
						.append("\" where key=\"")
						.append(key)
						.append("\"").toString();
				System.out.println(updateQuery);
				executeUpdate(updateQuery);
			}
		}
		mDatabaseConnection.commit();
		return new PutValueResponse(oldValue, seqToReturn);
	}

	@Override
	public String getValue(String key) throws SQLException {
		String queryForPresenceOfKey = "SELECT * FROM kvstore_schema where key=\"" + key + "\"";
		ResultSet res = executeQuery(queryForPresenceOfKey);
		int size = 0;
		String value = null;
		while(res.next()) {
			size++;
			value = res.getString("value");
		}
		assert(size == 0 || size == 1);
		if(size == 0) {
			return null;
		} else {
			return value;
		}
	}
	
	@Override
	public Integer getSequenceNumber(String key) {
		String queryForPresenceOfKey = "SELECT * FROM kvstore_schema where key=\"" + key + "\"";
		try {
			ResultSet res = executeQuery(queryForPresenceOfKey);
			int size = 0;
			Integer value = 0;
			while(res.next()) {
				size++;
				value = res.getInt("sequence_number");
			}
			assert(size == 0 || size == 1);
			if(size == 0) {
				return 0;
			} else {
				return value;
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	private int markAllKeyStale() throws SQLException {
		String query = "UPDATE kvstore_schema set might_be_stale=\"true\"";
		return (executeUpdate(query));
	}
	
	private void resendDirtyKeys() throws SQLException {
		String queryForPresenceOfKey = "SELECT key,value FROM kvstore_schema where dirty=\"true\"";
		try {
			ResultSet res = executeQuery(queryForPresenceOfKey);
			while(res.next()) {
				putValue(res.getString("key"), res.getString("value"), 
						PutValueRequest.APPLY_FOLLOWER_UPDATE, -1);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	private void establishConnection() {
		String jdbc_url = "jdbc:sqlite:" + mDbName;
		try {
			mDatabaseConnection = DriverManager.getConnection(jdbc_url);
			System.out.println(
					"Connection to SQLite has been established for client "
							+ mDbName);
			mDatabaseConnection.setAutoCommit(false);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	private int createDataStoreIfNotExists() throws SQLException {
		String query = "CREATE TABLE IF NOT EXISTS kvstore_schema(key char[128], value char[2048], "
				+ "sequence_number int, dirty boolean, might_be_stale boolean, PRIMARY KEY (key))";
		return (executeUpdate(query));
	}
	
	private int executeUpdate(String query) throws SQLException {
		Statement st = null;
		try {
			st = mDatabaseConnection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			return st.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	private ResultSet executeQuery(String query) throws SQLException {
		Statement st = null;
		try {
			st = mDatabaseConnection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			return st.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}