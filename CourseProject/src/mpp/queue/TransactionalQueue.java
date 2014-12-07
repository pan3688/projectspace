package mpp.queue;

import java.util.LinkedList;

import org.deuce.Atomic;

import mpp.exception.AbortedException;

public class TransactionalQueue extends LinkedList<Integer> implements IntQueue {

	@Atomic
	public boolean add(int value) throws AbortedException {
		return super.add(value);
	}

	@Atomic
	public int dequeue() throws AbortedException {
		return super.remove();
	}

	@Atomic
	public int head() throws AbortedException {
		return super.element();
	}

	@Override
	public void commit() throws AbortedException {

	}

	@Override
	public void abort() {

	}

	@Override
	public boolean nonTransactionalEnqueue(int value) {
		return super.add(value);
	}
}
