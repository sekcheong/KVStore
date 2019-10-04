package com.cs739.kvstore.datastore;

public enum PutValueRequest {
	APPLY_FOLLOWER_UPDATE,
	APPLY_PRIMARY_UPDATE,
	APPLY_FOLLOWER_PERSIST
}