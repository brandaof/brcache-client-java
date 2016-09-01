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
import java.net.Socket;
import java.util.zip.CRC32;

/**
 * Permite o armazenamento, atualização, remoção de um item em um servidor BrCache.
 * 
 * @author Brandao.
 */
public class BrCacheConnectionImp implements BrCacheConnection{
	
    public static final byte[] CRLF_DTA                  = "\r\n".getBytes();

    public static final byte[] DEFAULT_FLAGS_DTA         = "0".getBytes();
    
    public static final byte[] BOUNDARY_DTA              = "end".getBytes();
    
    public static final byte[] PUT_COMMAND_DTA           = "put".getBytes();

    public static final byte[] REPLACE_COMMAND_DTA       = "replace".getBytes();

    public static final byte[] GET_COMMAND_DTA           = "get".getBytes();

    public static final byte[] REMOVE_COMMAND_DTA        = "remove".getBytes();

    public static final byte[] BEGIN_TX_COMMAND_DTA      = "begin".getBytes();

    public static final byte[] COMMIT_TX_COMMAND_DTA     = "commit".getBytes();

    public static final byte[] ROLLBACK_TX_COMMAND_DTA   = "rollback".getBytes();
    
    public static final byte[] ERROR_DTA                 = "error".getBytes();
    
    public static final byte[] VALUE_RESULT_DTA          = "value".getBytes();
    
    public static final byte[] SUCCESS_DTA               = "ok".getBytes();

    public static final byte[] PUT_SUCCESS_DTA           = "stored".getBytes();
    
    public static final byte[] REPLACE_SUCCESS_DTA       = "replaced".getBytes();

    public static final byte[] NOT_STORED_DTA             = "not_stored".getBytes();
    
    public static final byte[] NOT_FOUND_DTA             = "not_found".getBytes();
    
    public static final byte[] SEPARATOR_COMMAND_DTA     = " ".getBytes();

    public static final String ENCODE                   = "UTF-8";
    
    private String host;
    
    private int port;
    
    private Socket socket;
    
    private StreamFactory streamFactory;
    
    private BRCacheSender sender;

    private BRCacheReceiver receiver;
    
    private boolean autocommit;
    
    public BrCacheConnectionImp(String host, int port){
        this(host, port, new DefaultStreamFactory());
        this.autocommit = true;
    }
    
    /**
     * Cria uma nova instância de {@link BrCacheConnection}
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     */
    public BrCacheConnectionImp(String host, int port, StreamFactory streamFactory){
        this.host = host;
        this.port = port;
        this.streamFactory = streamFactory;
    }
    
    public void connect() throws IOException{
        this.socket   = new Socket(this.getHost(), this.getPort());
        this.sender   = new BRCacheSender(socket, streamFactory, 8*1024);
        this.receiver = new BRCacheReceiver(socket, streamFactory, 8*1024);
    }
    
    public void disconect() throws IOException{
        
        if(this.socket != null)
            this.socket.close();
        
        this.sender   = null;
        this.receiver = null;
    }
    
	/* métodos de armazenamento */

	public boolean replace(
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException{
		
		try{
			this.sender.executeReplace(key, timeToLive, timeToIdle, value);
			return this.receiver.processReplaceResult();
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
    		throw new StorageException(0, "client error: unknow error", e);
		}
		
	}
    
	public boolean replace(
			String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws StorageException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			boolean result;
			if(o != null && o.equals(oldValue)){
				result = this.put(key, newValue, timeToLive, timeToIdle);
			}
			else
				result = false;
			
			this.commitLocalTransaction(localTransaction);
			return result;
		}
		catch(Throwable e){
			
			try{
				this.rollbackLocalTransaction(localTransaction);
			}
			catch(TransactionException ex){
				throw new StorageException(ex.getCode(), ex.getMessage(), e);
			}
			catch(Throwable ex){
				throw new StorageException(0, "rollback fail: " + ex.toString(), e);
			}
			
			if(e instanceof RecoverException){
				RecoverException r = (RecoverException)e;
				throw new StorageException(r.getCode(), r.getMessage(), r);
			}
			else
			if(e instanceof StorageException)
				throw (StorageException)e;
			else{
	    		throw new StorageException(0, "client error: unknow error", e);
			}
		}
		
	}
	
	public Object putIfAbsent(
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			if(o == null){
				this.put(key, value, timeToLive, timeToIdle);
			}
			
			this.commitLocalTransaction(localTransaction);
			return o;
		}
		catch(Throwable e){
			
			try{
				this.rollbackLocalTransaction(localTransaction);
			}
			catch(TransactionException ex){
				throw new StorageException(ex.getCode(), ex.getMessage(), e);
			}
			catch(Throwable ex){
				throw new StorageException(0, "rollback fail: " + ex.toString(), e);
			}
			
			if(e instanceof RecoverException){
				RecoverException r = (RecoverException)e;
				throw new StorageException(r.getCode(), r.getMessage(), r);
			}
			else
			if(e instanceof StorageException)
				throw (StorageException)e;
			else{
	    		throw new StorageException(0, "client error: unknow error", e);
			}
		}
		
	}
	
    public boolean put(String key, Object value, long timeToLive, long timeToIdle) 
            throws StorageException {

    	try{
	    	this.sender.executePut(key, timeToLive, timeToIdle, value);
	        return this.receiver.processPutResult();
    	}
    	catch(StorageException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new StorageException(0, "client error: unknow error", e);
    	}
        
    }

	/* métodos de coleta*/
    
    public Object get(String key, boolean forUpdate) throws RecoverException{
    	
    	try{
	    	this.sender.executeGet(key, forUpdate);
	        return this.receiver.processGetResult();
    	}
    	catch(RecoverException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new RecoverException(0, "client error: unknow error", e);
    	}
    	
    }
    
    public Object get(String key) throws RecoverException{
    	return this.get(key, false);
    }

    /* métodos de remoção */

	public boolean remove(
			String key, Object value) throws StorageException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			boolean result;
			if(o != null && o.equals(value)){
				result = this.remove(key);
			}
			else
				result = false;
			
			this.commitLocalTransaction(localTransaction);
			return result;
		}
		catch(Throwable e){
			
			try{
				this.rollbackLocalTransaction(localTransaction);
			}
			catch(TransactionException ex){
				throw new StorageException(ex.getCode(), ex.getMessage(), e);
			}
			catch(Throwable ex){
				throw new StorageException(0, "rollback fail: " + ex.toString(), e);
			}
			
			if(e instanceof RecoverException){
				RecoverException r = (RecoverException)e;
				throw new StorageException(r.getCode(), r.getMessage(), r);
			}
			else
			if(e instanceof StorageException)
				throw (StorageException)e;
			else{
	    		throw new StorageException(0, "client error: unknow error", e);
			}
		}
		
	}
    
    public boolean remove(String key) throws StorageException{

    	try{
	    	this.sender.executeRemove(key);
	        return this.receiver.processRemoveResult();
    	}
    	catch(StorageException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new StorageException(0, "client error: unknow error", e);
    	}
    	
    }
    
    public void setAutoCommit(boolean value) throws TransactionException{

    	try{
	    	if(value && !this.autocommit){
	    		this.sender.executeCommitTransaction();
	    		this.receiver.processCommitTransactionResult();
	    	}
	    	else
	    	if(this.autocommit){
	    		this.sender.executeBeginTransaction();
	    		this.receiver.processBeginTransactionResult();
	    	}
	    	
	    	this.autocommit = value;
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(0, "client error: unknow error", e);
    	}
    	
    }
    
    public boolean isAutoCommit(){
    	return this.autocommit;
    }
    
    public void commit() throws TransactionException{
    	try{
    		this.sender.executeCommitTransaction();
    		this.receiver.processCommitTransactionResult();
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(0, "client error: unknow error", e);
    	}
    }
    
    public void rollback() throws TransactionException{
    	try{
    		this.sender.executeCommitTransaction();
    		this.receiver.processCommitTransactionResult();
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(0, "client error: unknow error", e);
    	}
    }
    
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void close() throws IOException {
        this.socket.close();
    }

    public StreamFactory getStreamFactory() {
        return streamFactory;
    }
    
    private boolean startLocalTransaction() throws TransactionException{
    	if(this.isAutoCommit()){
    		this.setAutoCommit(false);
    		return true;
    	}
    	
    	return false;
    }

    private void commitLocalTransaction(Boolean local) throws TransactionException{
    	if(local != null && local){
    		this.commit();
    	}
    }

    private void rollbackLocalTransaction(Boolean local) throws TransactionException{
    	if(local != null && local){
    		this.rollback();
    	}
    }
    
    @SuppressWarnings("unused")
    @Deprecated
	private byte[] getCRC32(byte[] data, int off, int len){
        CRC32 crc32 = new CRC32();
        crc32.update(data, off, len);
        long crcValue = crc32.getValue();
        byte[] crc = new byte[8];

        crc[0] = (byte)(crcValue & 0xffL); 
        crc[1] = (byte)(crcValue >> 8  & 0xffL); 
        crc[2] = (byte)(crcValue >> 16 & 0xffL); 
        crc[3] = (byte)(crcValue >> 24 & 0xffL);
        crc[4] = (byte)(crcValue >> 32 & 0xffL); 
        crc[5] = (byte)(crcValue >> 40 & 0xffL); 
        crc[6] = (byte)(crcValue >> 48 & 0xffL); 
        crc[7] = (byte)(crcValue >> 56 & 0xffL);
        return crc;
    }
    
    protected void finalize() throws Throwable{
    	try{
    		if(this.socket != null){
    			this.socket.close();
    		}
    	}
    	finally{
    		super.finalize();
    	}
    }
}
