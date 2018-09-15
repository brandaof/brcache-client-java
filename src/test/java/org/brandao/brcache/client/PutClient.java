package org.brandao.brcache.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PutClient implements Runnable{
	
	private BrCacheConnectionPool pool;
	
	private AtomicInteger keyCount;

	private Throwable error;
	
	private int maxOperations;
	
	private CountDownLatch countDown;
	
	private String value;
	
	
	public PutClient(BrCacheConnectionPool pool, 
			AtomicInteger keyCount, int maxOperations, CountDownLatch countDown, String value){
		this.keyCount      = keyCount;
		this.maxOperations = maxOperations;
		this.pool          = pool;
		this.value         = value;
		this.countDown     = countDown;
	}
	
	public void run(){
		for(int i=0;i<maxOperations;i++){
			BrCacheConnection con = null;
			try{
				con = pool.getConnection();
				long key      = keyCount.getAndIncrement();
				String strKey = Long.toString(key, Character.MAX_RADIX);
				con.put(strKey, strKey + ":" + value, 0, 0);
			}
			catch(Throwable e){
				e.printStackTrace();
				error = e;
				break;
			}
			finally{
				if(con != null){
					try{
						con.close();
					}
					catch(Throwable ex){
						error = error != null? new Throwable(ex.toString(), error) : ex;
					}
				}
			}
		}
		
		countDown.countDown();
		
	}

	public Throwable getError() {
		return error;
	}
	
}
