package com.cs739.kvstore.datastore;

public class PutValueResponse {
	
	private String oldValue;
	private int sequenceNumber;
	
	public PutValueResponse(String oldValue, int seqno) {
		this.oldValue = oldValue;
		this.sequenceNumber = seqno;
	}
	
	public String getOldValue() {
		return oldValue;
	}
	
	public int getSequenceNumber() {
		return sequenceNumber;
	}
}