package mpp.queue;

import mpp.exception.AbortedException;

public interface IntQueue {

	public boolean add(int value) throws AbortedException;
	
	public int remove() throws AbortedException;
	
	public int element() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nonTransactionalEnqueue(int value);
	
}