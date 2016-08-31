package org.brandao.brcache.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class BRCacheReceiver {

    private BufferedInputStream in;
	
	public BRCacheReceiver(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.in = 
    			new BufferedInputStream(
    					streamFactory.createInpuStream(socket), bufferLength);
	}

	public boolean processPutResult() throws IOException, StorageException{
		
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
			throw new StorageException(new String(result));
		}
		
	}

	public boolean processReplaceResult() throws IOException, StorageException{
		
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
			throw new StorageException(new String(result));
		}
		
	}
	
	public List<Object> processGetResult() throws IOException, RecoverException{
		
		List<Object> result = new ArrayList<Object>();
		Object o;
		
		while((o = this.getObject()) != null){
			result.add(o);
		}
		
		return result;
	}
	
	private CacheEntry getObject() throws IOException, RecoverException{

		/*
		 * value <size> <flags>\r\n
		 * <data>\r\n
		 * end\r\n
		 */
		
		byte[] header = this.getLine();

		if(ArraysUtil.startsWith(header, BrCacheConnectionImp.VALUE_RESULT_DTA)){
			
			byte[][] dataParams = ArraysUtil.split(
					header, 
					BrCacheConnectionImp.VALUE_RESULT_DTA.length + 1, 
					(byte)32);
			
			String key       = new String(dataParams[0]);
			int size         = Integer.parseInt(new String(dataParams[1]));
			int flags        = Integer.parseInt(new String(dataParams[2]));
			byte[] dta       = new byte[size];
			
			this.in.read(dta, 0, dta.length);
			
			byte[] endData = new byte[2];
			this.in.read(endData, 0, endData.length);
			
			if(!Arrays.equals(BrCacheConnectionImp.CRLF_DTA, endData)){
				throw new IOException("corrupted data: " + key);
			}
			
			return new CacheEntry(key, size, flags, dta);
		}
		else
		if(Arrays.equals(BrCacheConnectionImp.BOUNDARY_DTA, header)){
			return null;
		}
		else{
			throw new RecoverException(new String(header));
		}
		
	}

	public boolean processRemoveResult() throws IOException, StorageException{
		
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
			throw new StorageException(new String(result));
		}
		
	}
	
	public void processBeginTransactionResult() throws IOException, StorageException{
		this.processDefaultResult();
	}

	public void processCommitTransactionResult() throws IOException, StorageException{
		this.processDefaultResult();
	}

	public void processRollbackTransactionResult() throws IOException, StorageException{
		this.processDefaultResult();
	}
	
	public void processDefaultResult() throws IOException, StorageException{
		
		/*
		 * ok | <error>
		 */
		byte[] result = this.getLine();

		if(!Arrays.equals(BrCacheConnectionImp.SUCCESS_DTA, result)){
			throw new StorageException(new String(result));
		}
		
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
			byte[] buf = new byte[i];
			this.in.read(buf, 0, buf.length);
			//return new String(buf, 0, buf.length - 2);
			return Arrays.copyOf(buf, buf.length - 2);
		}
		else{
			throw new IOException("premature end of data");
		}
		
	}
	
}
