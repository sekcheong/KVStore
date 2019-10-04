package com.cs739.kvstore.datastore;

import java.sql.SQLException;

public interface DataStore {
	/**
	 * Put a value into the datastore
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public PutValueResponse putValue(String key, String value, PutValueRequest type, int updatedSeqNumber) throws SQLException;

	/**
	 * Get a value from datastore
	 * 
	 * @param key
	 * @return
	 */
	public String getValue(String key) throws SQLException;
	
	public Integer getSequenceNumber(String key);
}