package org.brandao.brcache.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class BRCacheReceiver {

    private BufferedInputStream in;
	
	public BRCacheReceiver(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.in = 
    			new BufferedInputStream(
    					streamFactory.createInpuStream(socket), bufferLength);
	}

	public boolean processPutResult() throws IOException, CacheException{
		
		/*
		 * stored | replaced | <error>
		 */
		
		byte[] result = this.getLine();
		
		if(Arrays.equals(BrCacheConnectionImp.PUT_SUCCESS_DTA, result)){
			return false;
		}
		else
		if(Arrays.equals(BrCacheConnectionImp.REPLACE_SUCCESS_DTA, result)){
			return true;
		}
		else{
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}

	public boolean processReplaceResult() throws IOException, CacheException{
		
		/*
		 * replaced | not_stored | <error>
		 */
		
		byte[] result = this.getLine();
		
		if(Arrays.equals(BrCacheConnectionImp.NOT_STORED_DTA, result)){
			return false;
		}
		else
		if(Arrays.equals(BrCacheConnectionImp.REPLACE_SUCCESS_DTA, result)){
			return true;
		}
		else{
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}
	
	public Object processGetResult() throws IOException, CacheException, ClassNotFoundException{
		byte[] header = this.getLine();
		
		if(ArraysUtil.startsWith(header, BrCacheConnectionImp.VALUE_RESULT_DTA)){
			CacheEntry e = this.getObject(header);
			
			byte[] boundary = this.getLine();
			
			if(!Arrays.equals(BrCacheConnectionImp.BOUNDARY_DTA, boundary)){
				throw new IOException("expected end");
			}
			
			return e == null? null : this.toObject(e.getData());
		}
		else{
			Error err = this.parseError(header);
			throw new CacheException(err.code, err.message);
		}
	}

	public Map<String,Object> processGetsResult() throws IOException, CacheException, ClassNotFoundException{
		
		Map<String,Object> result = new HashMap<String, Object>();
		
		byte[] header = this.getLine();
		
		while(ArraysUtil.startsWith(header, BrCacheConnectionImp.VALUE_RESULT_DTA)){
			CacheEntry e = this.getObject(header);
			if(e != null){
				result.put(e.getKey(), this.toObject(e.getData()));
			}
		}
		
		if(!Arrays.equals(BrCacheConnectionImp.BOUNDARY_DTA, header)){
			Error err = this.parseError(header);
			throw new CacheException(err.code, err.message);
		}
		
		return result;
	}
	
	private CacheEntry getObject(byte[] header) throws IOException, CacheException{

		/*
		 * value <size> <flags>\r\n
		 * <data>\r\n
		 * end\r\n
		 */
		
		byte[][] dataParams = ArraysUtil.split(
				header, 
				BrCacheConnectionImp.VALUE_RESULT_DTA.length + 1, 
				(byte)32);
		
		String key       = new String(dataParams[0]);
		int size         = Integer.parseInt(new String(dataParams[1]));
		int flags        = Integer.parseInt(new String(dataParams[2]));
		
		if(size > 0){
			byte[] dta = new byte[size];
			this.in.read(dta, 0, dta.length);
			
			byte[] endData = new byte[2];
			this.in.read(endData, 0, endData.length);
			
			if(!Arrays.equals(BrCacheConnectionImp.CRLF_DTA, endData)){
				throw new IOException("corrupted data: " + key);
			}
			
			return new CacheEntry(key, size, flags, dta);
		}
		else{
			return null;
		}
		
	}

	public boolean processRemoveResult() throws IOException, CacheException{
		
		/*
		 * ok | not_found | <error>
		 */
		byte[] result = this.getLine();

		if(Arrays.equals(BrCacheConnectionImp.SUCCESS_DTA, result)){
			return true;
		}
		else
		if(Arrays.equals(BrCacheConnectionImp.NOT_FOUND_DTA, result)){
			return false;
		}
		else{
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}
	
	public void processBeginTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}

	public void processCommitTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}

	public void processRollbackTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}
	
	public void processDefaultTransactionCommandResult() throws IOException, CacheException{
		
		/*
		 * ok | <error>
		 */
		byte[] result = this.getLine();

		if(!Arrays.equals(BrCacheConnectionImp.SUCCESS_DTA, result)){
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}

	private Object toObject(byte[] data) throws ClassNotFoundException, IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream bin = new ObjectInputStream(in);
		return bin.readObject();
	}
	
	private Error parseError(byte[] value){
		String error   = new String(value);
		String codeSTR = error.substring(6, 10);
		String message = error.substring(12, error.length());
		return new Error(Integer.parseInt(codeSTR), message);
	}
	
	private byte[] getLine() throws IOException{
		
		int c;
		int i = 0;
		
		this.in.mark(256);
		
		while((c = this.in.read()) != -1 && c != '\n'){
			i++;
		}
	
		if(c == '\n'){
			this.in.reset();
			byte[] buf = new byte[i + 1];
			this.in.read(buf, 0, buf.length);
			//return new String(buf, 0, buf.length - 2);
			return Arrays.copyOf(buf, buf.length - 2);
		}
		else{
			throw new IOException("premature end of data");
		}
		
	}
	
	private static class Error{
		
		public int code;
		
		public String message;

		public Error(int code, String message) {
			super();
			this.code = code;
			this.message = message;
		}
		
	}
}
