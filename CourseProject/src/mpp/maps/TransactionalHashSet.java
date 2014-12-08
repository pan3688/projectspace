package mpp.maps;

import java.util.HashSet;

import org.deuce.Atomic;

import mpp.exception.AbortedException;

public class TransactionalHashSet extends HashSet<Integer> implements IntMap<Integer, Object> {
	
	public void begin(){
		
	}
	
	@Atomic
	public boolean put(Integer k, Object value) throws AbortedException{
		return super.add(k);
	}
	
	@Atomic
	public boolean containsSet(Integer k) throws AbortedException{
		return super.contains(k);
	}

	@Atomic
	public Object get(Integer k) throws AbortedException{
		
	}
	
	@Atomic
	public Object remove(Integer k) throws AbortedException{
		if(super.remove(k))
			return true;
		else
			return null;
	}
	
	@Override
	public void commit() throws AbortedException{
		
	}
	
	@Override
	public void abort(){
		
	}
	
	public boolean nonTransactionalPut(Integer k, Object v){
		return super.add(k);
	}
	
}
