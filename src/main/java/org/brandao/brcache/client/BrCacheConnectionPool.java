/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.client;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Controla as conexões com o servidor.
 * 
 * @author Brandao
 */
public class BrCacheConnectionPool {
    
    private int createdInstances;
    
    private final int minInstances;
    
    private final int maxInstances;

    private final String host;
    
    private final int port;
            
    private final BlockingQueue<BrCacheConnection> instances;

    private StreamFactory streamFactory;
    
    /**
     * Cria um novo pool de conexões.
     * 
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     * @param minInstances Conexões que serão iniciadas na criação da instância.
     * @param maxInstances Quantidade máxima de conexões que serão criadas.
     * @throws IOException Lançada se ocorrer alguma falha ao tentar iniciar as conexões.
     * @throws InterruptedException 
     */
    public BrCacheConnectionPool(String host, int port, int minInstances, 
            int maxInstances) 
            throws CacheException{

        this(host, port, minInstances, maxInstances, new DefaultStreamFactory());
    }
    
    public BrCacheConnectionPool(String host, int port, int minInstances, 
            int maxInstances, StreamFactory streamFactory) throws CacheException {

        if(minInstances < 0)
            throw new IllegalArgumentException("minInstances");
        
        if(maxInstances < 1)
            throw new IllegalArgumentException("maxInstances");
        
        if(minInstances > maxInstances)
            throw new IllegalArgumentException("minInstances");

        this.host             = host;
        this.port             = port;
        this.minInstances     = minInstances;
        this.maxInstances     = maxInstances;
        this.instances        = new ArrayBlockingQueue<BrCacheConnection>(this.maxInstances);
        this.createdInstances = 0;
        this.streamFactory    = streamFactory;
        
        for(int i=0;i<this.minInstances;i++){
            BrCacheConnection con = createConnection(host, port, streamFactory);
            this.instances.add(con);
            this.createdInstances++;
        }
        
    }
    
    private BrCacheConnection createConnection(String host, int port, StreamFactory streamFactory) throws CacheException{
        BrCacheConnectionImp con = new BrCacheConnectionImp(host, port, streamFactory);
        con.connect();
        return new BrCacheConnectionProxy(con, this);
        /*
            (BrCacheConnection)Proxy.newProxyInstance(
                getClass().getClassLoader(), 
                new Class[]{BrCacheConnection.class}, 
                new BrConnectionInvocationHandler(this, con));
        */
    }
    
    /**
     * Obtém uma conexão.
     * @return Conexão.
     * @throws CacheException Lançada se ocorrer uma falha ao tentar 
     * recuperar ou criar uma conexão.
     */
    public BrCacheConnection getConnection() throws CacheException{
        
    	try{
	        BrCacheConnection con = this.instances.poll();
	        
	        if(con != null)
	            return con;
	        else{
	            synchronized(this){
	                if(this.createdInstances < this.maxInstances){
	                    con = createConnection(host, port, this.streamFactory);
	                    this.createdInstances++;
	                    return con;
	                }
	            }
	            return this.instances.take();
	        }
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
        
    }

    /**
     * Tenta obtém uma conexão.
     * @return Conexão.
     * @throws CacheException Lançada se ocorrer uma falha ao tentar 
     * recuperar ou criar uma conexão.
     */
    public BrCacheConnection tryGetConnection(long l, TimeUnit tu) throws CacheException{
        
    	try{
	        BrCacheConnection con = this.instances.poll();
	        
	        if(con != null)
	            return con;
	        else{
	            synchronized(this){
	                if(this.createdInstances < this.maxInstances){
	                    con = createConnection(host, port, this.streamFactory);
	                    //((BrCacheConnectionImp)con).connect();
	                    this.createdInstances++;
	                    return con;
	                }
	                else
	                    return this.instances.poll(l, tu);
	            }
	        }
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
        
    }
    
    /**
     * Libera o uso da conexão.
     * @param con Conexão.
     */
    void release(BrCacheConnection con){
    	synchronized(this){
	        try{
	            this.instances.put(new BrCacheConnectionProxy(con, this));
	        }
	        catch(Throwable e){
	        	e.printStackTrace();
	        	this.shutdown(con);
	        }
    	}
    }

    /**
     * Remove a conexão do pool e libera o espaço para ser criada uma nova.
     * @param con Conexão.
     */
    void shutdown(BrCacheConnection con){
    	synchronized(this){
	    	try{
	    		con.close();
	    	}
	    	catch(Throwable ex){
	    		ex.printStackTrace();
	    	}
	    	finally{
		        if(this.instances.remove(con)){
		        	this.createdInstances--;
		        }
	    	}
    	}
    }

    /**
     * Destroi o pool de conexões.
     */
    public synchronized void shutdown(){
    }
    
}
