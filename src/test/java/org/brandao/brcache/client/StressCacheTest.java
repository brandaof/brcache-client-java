package org.brandao.brcache.client;

import java.awt.EventQueue;
import java.io.*;
import java.util.Random;
import junit.framework.TestCase;

import org.brandao.brcache.Configuration;
import org.brandao.brcache.collections.Collections;
import org.brandao.brcache.server.BrCacheServer;

/**
 *
 * @author Brandao
 */
public class StressCacheTest extends TestCase{
    
    private static String text = 
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh";
         
    private static int index = 0;
    
	private BrCacheServer server;
    
	@Override
	public void setUp(){
		Configuration config = new Configuration();
		this.server = new BrCacheServer(config);
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
    public void test() throws CacheException, InterruptedException {
        
    	final BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 1, 50);
        
        for(int i=0;i<4;i++){
            Thread th; 
            if(i % 2 == 0){
                th = new Thread(){
                    public void run(){
                        while(true){
                            try{
                                int rv = index++;
                                String key = String.valueOf(rv);
                                String value = key + text;
                                BrCacheConnection con = null;
                                try{
                                	con = pool.getConnection();
                                	con.put(key, value, 0, 0);
                                }
                                finally{
                                	if(con != null){
                                		con.close();
                                	}
                                }
                                
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                            index++;
                        }
                    }
                };
            }
            else{
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            int rv = r.nextInt(index);
                            String key = String.valueOf(rv);
                            String value = key + text;
                            BrCacheConnection con = null;

                            try{
	                            try{
	                            	con = pool.getConnection();
	                                String val = (String) con.get(key);
	                                if(val != null){
	                                    assertEquals(value, val);
	                                }
	                            }
	                            finally{
	                            	if(con != null){
	                            		con.close();
	                            	}
	                            }
                            }
                            catch(Throwable e){
                                e.printStackTrace();
                            }
                        }
                    }
                };
                
            }
            
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }    
    
}
