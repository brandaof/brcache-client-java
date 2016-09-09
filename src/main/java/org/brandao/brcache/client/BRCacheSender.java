package org.brandao.brcache.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

class BRCacheSender {
	
    private OutputStream out;
    
    public BRCacheSender(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.out = 
    			new BufferedOutputStream(bufferLength, streamFactory.createOutputStream(socket));
    }
    
	public void executePut(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 put <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
			 end\r\n 
		 */
		
		byte[] data = this.toBytes(value);
		
		out.write(BrCacheConnectionImp.PUT_COMMAND_DTA);
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(ArraysUtil.toBytes(key));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(ArraysUtil.toBytes(timeToLive));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(ArraysUtil.toBytes(timeToIdle));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(ArraysUtil.toBytes(data.length));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(BrCacheConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(BrCacheConnectionImp.BOUNDARY_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
		
	}
	
	public void executeReplace(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 put <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
			 end\r\n 
		 */

		byte[] data = this.toBytes(value);
		
		out.write(BrCacheConnectionImp.REPLACE_COMMAND_DTA);
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToLive).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToIdle).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(Integer.toString(data.length).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(BrCacheConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(BrCacheConnectionImp.BOUNDARY_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeSet(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 set <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
			 end\r\n 
		 */

		byte[] data = this.toBytes(value);
		
		out.write(BrCacheConnectionImp.SET_COMMAND_DTA);
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToLive).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToIdle).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(Integer.toString(data.length).getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(BrCacheConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.write(BrCacheConnectionImp.BOUNDARY_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeGet(String key, boolean forUpdate) throws IOException{
		/*
			get <key> <update> <reserved>\r\n
		 */
		
		out.write(BrCacheConnectionImp.GET_COMMAND_DTA);
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(forUpdate? '1' : '0');
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(BrCacheConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeRemove(String key) throws IOException{
		
		/*
			delete <name> <reserved>\r\n
		 */
		
		out.write(BrCacheConnectionImp.REMOVE_COMMAND_DTA);	
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(BrCacheConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeBeginTransaction() throws IOException{
		
		/*
			begin\r\n
		 */
		
		out.write(BrCacheConnectionImp.BEGIN_TX_COMMAND_DTA);	
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeCommitTransaction() throws IOException{
		
		/*
			commit\r\n
		 */
		
		out.write(BrCacheConnectionImp.COMMIT_TX_COMMAND_DTA);	
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeRollbackTransaction() throws IOException{
		
		/*
			rollback\r\n
		 */
		
		out.write(BrCacheConnectionImp.ROLLBACK_TX_COMMAND_DTA);	
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeShowVar(String var) throws IOException{
		
		/*
			show_var <var_name>\r\n
		 */
		
		out.write(BrCacheConnectionImp.SHOW_VAR);	
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(var.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeSetVar(String var, Object value) throws IOException{
		
		/*
			set_var <var_name> <var_value>\r\n
		 */
		
		String strValue = String.valueOf(value);
		
		out.write(BrCacheConnectionImp.SET_VAR);	
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(var.getBytes(BrCacheConnectionImp.ENCODE));
		out.write(BrCacheConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(strValue.getBytes(BrCacheConnectionImp.ENCODE));	
		out.write(BrCacheConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	private byte[] toBytes(Object value) throws IOException{
        ObjectOutputStream out     = null;
    	ByteArrayOutputStream bout = null;
    	
        try{
            bout = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bout);
            out.writeObject(value);
            out.flush();
            return bout.toByteArray();
        }
        finally{
            try{
                if(out != null){
                    out.close();
                }
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
		
	}
}
