package mpp.stack;

import mpp.exception.AbortedException;

public interface IntStack {

	public boolean pushStack(int value) throws AbortedException;
	
	public int popStack() throws AbortedException;
	
	public int topStack() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nonTransactionalPush(int value);
	
}

















