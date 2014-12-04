package mpp.maps;

import mpp.exception.AbortedException;

public interface IntMap<K,V> {

	public boolean put(K k,V v) throws AbortedException;
 
	public V get(K k) throws AbortedException;
	
	public boolean contains(K k) throws AbortedException;
	
	public boolean commit() throws AbortedException;
	
	public boolean abort();
	
	public boolean nonTransactionalPut(K k,V v);
}
