package org.brandao.brcache.client;

public class TXCacheHelper {

	public static abstract class ConcurrentTask extends Thread{
		
		private Throwable error;
		
		private BrCacheConnection cache;
		
		private Object value;
		
		private String key;
		
		private Object value2;
		
		public ConcurrentTask(BrCacheConnection cache, String key,
				Object value, Object value2) {
			this.cache  = cache;
			this.value  = value;
			this.key    = key;
			this.value2 = value2;
		}

		public void run(){
			try{
				this.execute(cache, key, value, value2);
			}
			catch(Throwable e){
				this.error = e; 
			}
		}

		protected abstract void execute(BrCacheConnection cache, String key, Object value,
				Object value2) throws Throwable;
		
		public Throwable getError() {
			return error;
		}
		
	}
}
