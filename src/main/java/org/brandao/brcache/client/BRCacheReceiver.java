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

	public boolean processPutResult() throws IOException, StorageException{
		
		/*
		 * stored | replaced | <error>
		 */
		
		try{
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
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		
	}

	public boolean processReplaceResult() throws IOException, StorageException{
		
		/*
		 * replaced | not_stored | <error>
		 */
		
		try{
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
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
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
			
			String paramsSTR = 
				new String(
						header, 
						BrCacheConnectionImp.VALUE_RESULT_DTA.length + 1, 
						header.length);

			String[] params  = paramsSTR.split(" ");
			String key       = params[0];
			int size         = Integer.parseInt(params[1]);
			int flags        = Integer.parseInt(params[2]);
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
		if(header.equals(BrCacheConnectionImp.BOUNDARY)){
			return null;
		}
		else{
			throw new RecoverException(header);
		}
		
	}

	public boolean processRemoveResult() throws IOException, StorageException{
		
		try{
			String result = this.getLine();

			if(BrCacheConnectionImp.SUCCESS.equals(result)){
				return true;
			}
			else
			if(BrCacheConnectionImp.NOT_FOUND.equals(result)){
				return false;
			}
			else{
				throw new StorageException(result);
			}
		}
		catch(StorageException e){
			throw e;
		}
		
	}
	
}
