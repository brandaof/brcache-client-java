package org.brandao.brcache.client;

/**
 * Lançada se ocorrer alguma falha ao processar uma transação.
 * 
 * @author Brandao
 *
 */
public class TransactionException 
	extends Exception{

	private static final long serialVersionUID = 4216893703188841533L;

	public TransactionException() {
		super();
	}

	public TransactionException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TransactionException(String message, Throwable cause) {
		super(message, cause);
	}

	public TransactionException(String message) {
		super(message);
	}

	public TransactionException(Throwable cause) {
		super(cause);
	}

}
