package org.brandao.brcache.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

class BRCacheReceiver {

    private BufferedInputStream in;
	
	public BRCacheReceiver(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.in = 
    			new BufferedInputStream(
    					streamFactory.createInpuStream(socket), bufferLength);
	}
	
	public void putResult() throws StorageException{
		
		try{
			this.in.mark(256);
			
			int c;
			int i = 0;
			while((c = this.in.read()) != -1 && c != '\n'){
				i++;
			}
			
			if(c != -1){
				
				this.in.reset();
				byte[] buf = new byte[i];
				this.in.read(buf, 0, buf.length - 2);
				
				String result = new String(buf);
				
				if(!BrCacheConnectionImp.SUCCESS.equals(result)){
					throw new StorageException(new String(buf));
				}
				
			}
			else{
				throw new IOException("premature end of data");
			}
			
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		
	}

	public Object getGetResult() throws IOException{
		
		this.in.mark(256);
		
		int c;
		int i = 0;
		
		while((c = this.in.read()) != -1 && c != '\n'){
			i++;
		}
		
		if(c != -1){
			this.in.reset();
			byte[] buf = new byte[i];
			this.in.read(buf, 0, buf.length - 2);
			String header = new String(buf);
			
		}
		else{
			throw new IOException("premature end of data");
		}
		
	}
	
	public Object getObject() throws IOException{
		
		this.in.mark(256);
		
		int c;
		int i = 0;
		
		while((c = this.in.read()) != -1 && c != '\n'){
			i++;
		}
		
		if(c != -1){
			this.in.reset();
			byte[] buf = new byte[i];
			this.in.read(buf, 0, buf.length - 2);
			String header = new String(buf);
			
			if(header.startsWith(BrCacheConnectionImp.VALUE_RESULT)){
				String
			}
			else
			if(header.equals(BrCacheConnectionImp.BOUNDARY)){
				return null;
			}
			else
				throw new IOException("expected END: " + header);
		}
		else{
			throw new IOException("premature end of data");
		}
		
	}

}
