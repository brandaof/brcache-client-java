package org.brandao.brcache.client;

/**
 * Lançada se ocorrer alguma falha ao processar uma transação.
 * 
 * @author Brandao
 *
 */
public class TransactionException 
	extends CacheException{

	private static final long serialVersionUID = 5868004710643964867L;

	public TransactionException(int code, String string, Throwable thrwbl) {
		super(code, string, thrwbl);
	}

	public TransactionException(int code, String string) {
		super(code, string);
	}


}
