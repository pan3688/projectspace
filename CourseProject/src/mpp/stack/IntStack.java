package mpp.stack;

import mpp.exception.AbortedException;

public interface IntStack {

	public boolean push(int value) throws AbortedException;
	
	public int pop() throws AbortedException;
	
	public int top() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nonTransactionalPush(int value);
	
}

















