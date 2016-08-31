package org.brandao.brcache.client;

/**
 * Provê métodos auxiliares de manipulação de arranjo de bytes.
 * @author Brandao
 *
 */
public class ArraysUtil {

	/**
	 * Verifica se um arranjo de bytes inicia com os mesmos bytes de outro arranjo de bytes.
	 * @param array arranjo de bytes que será verificado.
	 * @param value arranjo de bytes usado na comparação
	 * @return <code>true</code> se array iniciar com value. Caso contrário <code>false</code>
	 */
	public static boolean startsWith(byte[] array, byte[] value){
		
		if(array.length < value.length)
			return false;
		
		for(int i=0;i<value.length;i++){
			
			if(array[i] != value[i]){
				return false;
			}
				
		}
		
		return true;
	}
	
}
