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

import java.io.Serializable;

import org.brandao.brcache.client.BrCacheClient;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;

/**
 * Faz com que entidades sejam enviadas para um determinado cache.
 * 
 * @author Brandao
 */
public class CacheSwapper 
    implements Swapper, Serializable{

	private static final long serialVersionUID = -1342760584177753822L;

	private String id;
    
    private long maxalive;
    
    private long maxIndex;
    
    private BrCacheClient client;
    
    public CacheSwapper(BrCacheClient client){
    	this.client = client;
        this.maxIndex = -1;
    }
    
    public void setId(String value) {
        this.id = value;
    }

    public void sendItem(long index, Entry<?> item) {
        synchronized(this){
            if(this.maxIndex < index)
                this.maxIndex = index;
        }

        if(item.getItem() == null)
            throw new IllegalStateException("invalid item: " + index);
        
        try{
            this.sendItem(index, item.getItem());
        }
        catch (StorageException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void sendItem(long index, Object item) throws StorageException {
        client.put(new String(this.id + ":" + index), this.getMaxalive(), item);
    }
    
    public Entry<?> getItem(long index) {
        try{
            Object item;
                item = client.get(new String(this.id + ":" + index));
            
            if(item == null)
                return null;
            
            Entry<?> entry = new Entry<Object>(index, false, item);
            entry.setNeedReload(false);
            return entry;
        }
        catch (RecoverException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long getMaxalive() {
        return maxalive;
    }

    public void setMaxalive(long maxalive) {
        this.maxalive = maxalive;
    }

    public void clear() {
        try{
            this.clear(this.client);
        }
        catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private void clear(BrCacheClient client) throws RecoverException, StorageException{
        for(int i=0;i<=this.maxIndex;i++){
            client.remove(new String(this.id + ":" + i));
        }
    }
    
}
