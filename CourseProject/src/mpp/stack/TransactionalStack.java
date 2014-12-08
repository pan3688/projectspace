package mpp.stack;

import java.util.Stack;

import mpp.exception.AbortedException;

import org.deuce.Atomic;

public class TransactionalStack extends Stack<Integer> implements IntStack {

	@Atomic
	public boolean pushStack(int value) throws AbortedException {
		if(super.push(value) == value)
			return true;
		else
			return false;
	}

	@Atomic
	public int popStack() throws AbortedException {
		return super.pop();
	}

	@Atomic
	public int topStack() throws AbortedException {
		return super.peek();
	}

	@Override
	public void commit() throws AbortedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub

	}

	@Atomic
	public boolean nonTransactionalPush(int value) {
		if(super.push(value) == value)
			return true;
		
		return false;
	}

}
