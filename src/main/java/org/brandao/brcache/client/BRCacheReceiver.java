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
	
	private String getLine() throws IOException{
		
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
			return new String(buf, 0, buf.length - 2);
		}
		else{
			throw new IOException("premature end of data");
		}
		
	}

	public void processPutResult() throws IOException, StorageException{
		
		try{
			String result = this.getLine();
			
			if(!BrCacheConnectionImp.SUCCESS.equals(result)){
				throw new StorageException(result);
			}
		}
		catch(StorageException e){
			throw e;
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

		String header = this.getLine();

		if(header.startsWith(BrCacheConnectionImp.VALUE_RESULT)){
			
			String paramsSTR = header.substring(BrCacheConnectionImp.VALUE_RESULT.length());
			String[] params  = paramsSTR.split(" ");
			String key = params[0];
			
			int size   = Integer.parseInt(params[1]);
			int flags  = Integer.parseInt(params[2]);
			byte[] dta = new byte[size];
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
