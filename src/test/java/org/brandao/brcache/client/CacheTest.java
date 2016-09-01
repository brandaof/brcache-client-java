package org.brandao.brcache.client;

import java.awt.EventQueue;
import org.brandao.brcache.Configuration;
import org.brandao.brcache.server.BrCacheServer;

import junit.framework.TestCase;

public class CacheTest extends TestCase{

	private static final String SERVER_HOST	= "localhost";

	private static final int SERVER_PORT	= 8084;
	
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
	
	public void testReplace() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
		con.close();
	}

	public void testReplaceSuccess() throws Throwable{
		String prefixKEY = "testReplaceSuccess:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testReplaceExact() throws Throwable{
		String prefixKEY = "testReplaceSuccess:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testReplaceExactSuccess:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws Throwable{
		String prefixKEY = "testputIfAbsent:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testputIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testputIfAbsentExistValue:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}
	
	/* put */
	
	public void testPut() throws Throwable{
		String prefixKEY = "testPut:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testGet() throws Throwable{
		String prefixKEY = "testGet:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testGetOverride() throws Throwable{
		String prefixKEY = "testGetOverride:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws Throwable{
		String prefixKEY = "testRemoveExact:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();

		TestCase.assertNull(con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
	}

	public void testRemove() throws Throwable{
		String prefixKEY = "testRemove:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws Throwable{
		String prefixKEY = "testTimeToLive:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 1000, 0);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(400);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testTimeToLiveLessThanTimeToIdle() throws Throwable{
		String prefixKEY = "testTimeToLiveLessThanTimeToIdle:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 1000, 5000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testNegativeTimeToLive() throws Throwable{
		String prefixKEY = "testNegativeTimeToLive:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		try{
			con.put(prefixKEY + KEY, VALUE, -1, 5000);
			fail();
		}
		catch(CacheException e){
			if(e.getCode() != 1003 || !e.getMessage().equals("timeToLive is invalid!")){
				fail();
			}
				
		}
	}

	/* TimeToIdle */
	
	public void testTimeToIdle() throws Throwable{
		String prefixKEY = "testTimeToIdle:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 0, 1000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
		
	}

	public void testTimeToIdleLessThanTimeToLive() throws Throwable{
		String prefixKEY = "testTimeToIdleLessThanTimeToLive:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 20000, 1000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testNegativeTimeToIdle() throws Throwable{
		String prefixKEY = "testNegativeTimeToIdle:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		try{
			con.put(prefixKEY + KEY, VALUE, 0, -1);
			fail();
		}
		catch(CacheException e){
			if(e.getCode() != 1003 || !e.getMessage().equals("timeToIdle is invalid!")){
				fail();
			}
				
		}
	}
	
}
