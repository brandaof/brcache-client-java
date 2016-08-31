package org.brandao.brcache.client;

import java.util.ArrayList;
import java.util.List;

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

	/**
	 * Fragmento um arranjo usando um byte como delimitador.
	 * @param array arranjo
	 * @param value delimitador.
	 * @param index índice inicial.
	 * @return fragmentos.
	 */
	public static byte[][] split(byte[] array, int index, byte value){
		int start = index;
		int end   = 0;
		List<byte[]> result = new ArrayList<byte[]>();
		int limit = array.length -1;
		
		for(int i=index;i<array.length;i++){
			
			if(array[i] == value){
				end = i;
				byte[] item = copy(array, start, end);
				result.add(item);
				start = end + 1;
				end = start;
			}
			
		}
		
		if(start == limit && array[limit] != 32){
			byte[] item = copy(array, start, start + 1);
			result.add(item);
		}
		
		return result.toArray(new byte[0][]);
	}
	
	private static byte[] copy(byte[] origin, int start, int end){
		int len = end-start;
		byte[] item = new byte[end-start];
		System.arraycopy(origin, start, item, 0, len);
		return item;
	}
	
}
