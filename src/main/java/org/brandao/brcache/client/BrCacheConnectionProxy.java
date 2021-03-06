package org.brandao.brcache.client;

class BrCacheConnectionProxy implements BrCacheConnection{

	private BrCacheConnectionPool pool;
	
	private BrCacheConnection con;
	
	private boolean closed;
	
	public BrCacheConnectionProxy(BrCacheConnection con, 
			BrCacheConnectionPool pool) throws CacheException {
		this.pool   = pool;
		this.con    = con;
		this.closed = false;
	}

	public void close() throws CacheException {
		if(!this.closed){
			this.pool.release(con);
			this.closed = true;
		}
	}

	public boolean isClosed() {
		return this.closed;
	}

	public boolean replace(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.replace(key, value, timeToLive,
				timeToIdle);
	}

	public boolean replace(String key, Object oldValue, Object newValue,
			long timeToLive, long timeToIdle) throws CacheException {
		return this.con.replace(key, oldValue, newValue,
				timeToLive, timeToIdle);
	}

	public Object putIfAbsent(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.putIfAbsent(key, value, timeToLive,
				timeToIdle);
	}

	public boolean set(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.set(key, value, timeToLive,
				timeToIdle);
	}

	public boolean put(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.put(key, value, timeToLive,
				timeToIdle);
	}

	public Object get(String key, boolean forUpdate) throws CacheException {
		return this.con.get(key, forUpdate);
	}

	public Object get(String key) throws CacheException {
		return this.con.get(key);
	}

	public boolean remove(String key, Object value) throws CacheException {
		return this.con.remove(key, value);
	}

	public boolean remove(String key) throws CacheException {
		return this.con.remove(key);
	}

	public void setAutoCommit(boolean value) throws CacheException {
		this.con.setAutoCommit(value);
	}

	public boolean isAutoCommit() throws CacheException {
		return this.con.isAutoCommit();
	}

	public void commit() throws CacheException {
		this.con.commit();
	}

	public void rollback() throws CacheException {
		this.con.rollback();
	}

	public String getHost() {
		return this.con.getHost();
	}

	public int getPort() {
		return this.con.getPort();
	}

    protected void finalize() throws Throwable {
		try{
			this.close();
		}
		finally{
			super.finalize();
		}
	}
}
