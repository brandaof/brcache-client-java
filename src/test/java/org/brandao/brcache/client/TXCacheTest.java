package org.brandao.brcache.client;

import java.awt.EventQueue;
import java.io.IOException;
import java.io.InputStream;

import org.brandao.brcache.CacheTestHelper;
import org.brandao.brcache.Configuration;
import org.brandao.brcache.TXCacheHelper.ConcurrentTask;
import org.brandao.brcache.server.BrCacheServer;
import org.brandao.brcache.tx.CacheTransaction;
import org.brandao.brcache.tx.TXCache;

import junit.framework.TestCase;

public class TXCacheTest extends TestCase{

	private static final String SERVER_HOST	= "localhost";

	private static final int SERVER_PORT	= 8084;
	
	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
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
	}

	public void testReplaceSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	public void testReplaceExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testputIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* put */
	
	public void testPut() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testGet() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testGetOverride() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}

	public void testRemove() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}
	
	/* with explicit transaction */

	/* replace */
	
	public void testExplicitTransactionReplace() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
		con.commit();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
	}

	public void testExplicitTransactionReplaceSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionReplaceExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		con.commit();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
	}

	public void testExplicitTransactionReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testExplicitTransactionPutIfAbsent() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionPutIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* put */
	
	public void testExplicitTransactionPut() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testExplicitTransactionGet() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionGetOverride() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testExplicitTransactionRemoveExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		con.commit();
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
	}

	public void testExplicitTransactionRemove() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}	
	
	/* concurrent transaction*/
	
	/* replace */
	
	public void testConcurrentTransactionReplace() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(con, prefixKEY + KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.putStream(prefixKEY + KEY, (InputStream)value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.get(prefixKEY + KEY, true));
		task.start();
		Thread.sleep(2000);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionReplaceSuccess() throws Throwable{
		final String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionReplaceStream() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				con.putStream(prefixKEY + KEY, (InputStream)value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.getStream(prefixKEY + KEY, true));
		task.start();
		Thread.sleep(2000);
		TestCase.assertFalse(con.replaceStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
	}

	public void testConcurrentTransactionReplaceStreamSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.putStream(prefixKEY + KEY, (InputStream)value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		con.putStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
		TestCase.assertTrue(con.replaceStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE2), 0, 0));
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
		
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
	}
	
	public void testConcurrentTransactionReplaceExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.get(prefixKEY + KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testConcurrentTransactionPutIfAbsent() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.get(prefixKEY + KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionPutIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionPutIfAbsentStream() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.putStream(prefixKEY + KEY, (InputStream)value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.getStream(prefixKEY + KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(con.putIfAbsentStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
	}

	public void testConcurrentTransactionPutIfAbsentStreamExistValue() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.putStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.putIfAbsentStream(prefixKEY + KEY, CacheTestHelper.toStream(VALUE2), 0, 0)));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(con.getStream(prefixKEY + KEY)));
	}	
	/* put */
	
	public void testConcurrentTransactionPut() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testConcurrentTransactionGet() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionGetOverride() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testConcurrentTransactionRemoveExact() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		con.put(prefixKEY + KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionRemove() throws Throwable{
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		

		ConcurrentTask task = new ConcurrentTask(cache, prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String prefixKEY + KEY, Object value,
					Object value2) throws Throwable {
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws Throwable{
		String prefixKEY = "testReplace:";
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
		String prefixKEY = "testReplace:";
		BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
		con.connect();
		
		con.put(prefixKEY + KEY, VALUE, 1000, 5000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testNegativeTimeToLive() throws Throwable{
		try{
			String prefixKEY = "testReplace:";
			BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
			con.connect();
			
			con.put(prefixKEY + KEY, VALUE, -1, 5000);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1029)){
				fail();
			}
				
		}
	}

	/* TimeToIdle */
	
	public void testTimeToIdle() throws Throwable{
		String prefixKEY = "testReplace:";
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
		String prefixKEY = "testReplace:";
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
		try{
			String prefixKEY = "testReplace:";
			BrCacheConnection con = new BrCacheConnectionImp(SERVER_HOST, SERVER_PORT);
			con.connect();
			
			con.put(prefixKEY + KEY, VALUE, 0, -1);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1028)){
				fail();
			}
				
		}
	}
	
}
