package mpp.maps;

import mpp.exception.AbortedException;

public interface IntMap<K,V> {
	
	public void begin();

	public boolean put(K k,V v) throws AbortedException;
 
	public V get(K k) throws AbortedException;
	
	public boolean contains(K k) throws AbortedException;
	
	public V remove(K k) throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public boolean abort();
	
	public boolean nonTransactionalPut(K k,V v);
}
