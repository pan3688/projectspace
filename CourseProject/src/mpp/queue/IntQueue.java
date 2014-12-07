package mpp.queue;

import mpp.exception.AbortedException;

public interface IntQueue {

	public boolean add(int value) throws AbortedException;
	
	public int dequeue() throws AbortedException;
	
	public int head() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nonTransactionalEnqueue(int value);
	
}