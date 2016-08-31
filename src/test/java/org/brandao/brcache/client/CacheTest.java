package org.brandao.brcache.client;

import java.awt.EventQueue;
import java.io.IOException;

import org.brandao.brcache.Configuration;
import org.brandao.brcache.server.BrCacheServer;

import junit.framework.TestCase;

public class CacheTest extends TestCase{

	private static final String SERVER_HOST	= "localhost";

	private static final int SERVER_PORT	= 9090;
	
	private static final String KEY			= "teste";

	private static final String VALUE		= "value";

	private static final String VALUE2		= "val";
	
	private BrCacheServer server;
	
	@Override
	public void setUp(){
		this.server = new BrCacheServer(new Configuration());
		EventQueue.invokeLater(new Runnable(){

			public void run() {
				try{
					server.start();
				}
				catch(Throwable e){
					e.printStackTrace();
				}
				
			}
			
		});
		
	}

	@Override
	public void tearDown(){
		try{
			this.server.stop();
		}
		catch(Throwable e){
			e.printStackTrace();
		}
	}
	
	/* replace */
	
	public void testReplace() throws StorageException, IOException{
		BrCacheConnection con = new BrCacheConnectionImp("localhost", 9090);
		TestCase.assertFalse(con.replace(KEY, VALUE, 0, 0));
		con.close();
	}

	public void testReplaceSuccess() throws StorageException, RecoverException{
		String prefixKEY = "testReplaceSuccess:";
		BrCacheConnection con = new BrCacheConnectionImp("localhost", 9090);
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testReplaceStream() throws StorageException, IOException{
		Cache cache = new Cache();
		TestCase.assertFalse(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
	}

	public void testReplaceStreamSuccess() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		Cache cache = new Cache();
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, (String)CacheTestHelper.toObject(cache.getStream(KEY)));
		TestCase.assertTrue(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0));
		TestCase.assertEquals(VALUE2, (String)CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	public void testReplaceExact() throws StorageException{
		Cache cache = new Cache();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws StorageException, RecoverException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws StorageException, RecoverException{
		Cache cache = new Cache();
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentExistValue() throws StorageException, RecoverException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentStream() throws StorageException, RecoverException, IOException{
		Cache cache = new Cache();
		TestCase.assertNull(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentStreamExistValue() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		Cache cache = new Cache();
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0)));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	/* put */
	
	public void testPut() throws StorageException, RecoverException{
		Cache cache = new Cache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testGet() throws StorageException, RecoverException{
		Cache cache = new Cache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testGetOverride() throws StorageException, RecoverException{
		Cache cache = new Cache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws StorageException, RecoverException{
		Cache cache = new Cache();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
	}

	public void testRemove() throws StorageException, RecoverException{
		Cache cache = new Cache();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertTrue(cache.remove(KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws InterruptedException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 1000, 0);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(400);
		assertNull(cache.get(KEY));
	}

	public void testTimeToLiveLessThanTimeToIdle() throws InterruptedException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 1000, 5000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
	}

	public void testNegativeTimeToLive() throws InterruptedException{
		try{
			Cache cache = new Cache();
			cache.put(KEY, VALUE, -1, 5000);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1029)){
				fail();
			}
				
		}
	}

	/* TimeToIdle */
	
	public void testTimeToIdle() throws InterruptedException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 0, 1000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
		
	}

	public void testTimeToIdleLessThanTimeToLive() throws InterruptedException{
		Cache cache = new Cache();
		cache.put(KEY, VALUE, 20000, 1000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
	}

	public void testNegativeTimeToIdle() throws InterruptedException{
		try{
			Cache cache = new Cache();
			cache.put(KEY, VALUE, 0, -1);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1028)){
				fail();
			}
				
		}
	}
	
}
