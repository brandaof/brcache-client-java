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

import java.io.Closeable;
import java.io.IOException;

import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

/**
 * Permite o armazenamento, atualização, remoção de um item em um servidor BrCache.
 * 
 * @author Brandao.
 */
public interface BrCacheConnection extends Closeable{
    
    /**
     * Faz a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * conectar ao servidor.
     */
    void connect() throws IOException;
    
    /**
     * Fecha a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * fechar a conexão com o servidor.
     */
    void disconect() throws IOException;
    
	/**
	 * Associa o valor à chave.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
    void put(String key, long timeToLive, long timeToIdle, Object value) throws StorageException;
    
	/**
     * Obtém o valor associado à chave bloqueando ou não 
     * seu acesso as demais transações.
     * @param key chave associada ao valor.
     * @param forUpdate <code>true</code> para bloquear o item. Caso contrário <code>false</code>.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
    Object get(String key, boolean forUpdate) throws RecoverException;

	/**
	 * Remove o valor associado à chave.
	 * @param key chave associada ao valor.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
	 */
    boolean remove(String key) throws StorageException;
    
    /**
     * Obtém o endereço do servidor.
     * @return Endereço do servidor.
     */
    String getHost();

    /**
     * Obtém a porta do servidor.
     * @return Porta do servidor.
     */
    int getPort();
    

}
