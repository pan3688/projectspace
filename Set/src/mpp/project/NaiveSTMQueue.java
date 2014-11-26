package mpp.project;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.deuce.Atomic;

public class NaiveSTMQueue<T> {

	ConcurrentLinkedQueue<T> q = new ConcurrentLinkedQueue<T>();
	
	@Atomic
	public void enqueue(T item){
		q.add(item);
	}
	
	public void enqDeq(){
		
	}
}
