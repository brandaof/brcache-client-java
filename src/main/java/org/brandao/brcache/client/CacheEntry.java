package org.brandao.brcache.client;

public class CacheEntry {

	private String key;
	
	private int size;
	
	private int flags;
	
	private byte[] data;

	public CacheEntry(String key, int size, int flags, byte[] data) {
		this.key = key;
		this.size = size;
		this.flags = flags;
		this.data = data;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
}
