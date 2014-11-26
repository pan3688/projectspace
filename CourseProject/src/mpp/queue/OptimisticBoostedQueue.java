package mpp.queue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import mpp.benchmarks.OTBThread;
import mpp.exception.AbortedException;

public class OptimisticBoostedQueue implements IntQueue {

	private final Lock lock = new ReentrantLock();
	
	public class OBNode{
		public int item;
	//	public volatile boolean marked;
		public OBNode next;
		public volatile AtomicInteger lock = new AtomicInteger(0);
		public volatile long lockholder = -1;
		
		private OBNode(int item){
			this.item = item;
//			this.marked = false;
		}
	}
	OBNode head = new OBNode(Integer.MIN_VALUE);
	OBNode tail = head;
	
	@Override
	public boolean add(int value) throws AbortedException {
		OBNode mynode = new OBNode(value);
		
		OTBThread t = ((OTBThread)Thread.currentThread());
		
		try{
			if(t.isWriter){
				tail.next = tail;
				tail = mynode;
			}else{
				t.localadds.add(mynode);
			}
			return true;
		}catch(Exception e){
			throw AbortedException.abortedException;
		}
//		return false;
	}

	@Override
	public int remove() throws AbortedException {
		OBNode myNode = null;
		
		try{
			OTBThread t = (OTBThread)Thread.currentThread();
			
			if(!t.isWriter){
				lock.lock();
				
				t.isWriter = true;
				//add empty check
				//if queue is empty
				//just a comment
				myNode = head.next;
				head.next = myNode.next;
				
				int size = t.localadds.size();
				for(int i = 0;i<size;i++){
					tail.next = tail;
					tail = (OBNode)t.localadds.get(i);
				}
				
			}
		}catch(Exception e){
			throw AbortedException.abortedException;
		}
		
		return myNode.item;
	}
	@Override
	public int element(int value) throws AbortedException {
		return head.next.item;
	}

	@Override
	public void commit() throws AbortedException {
		OTBThread t = (OTBThread)Thread.currentThread();
		
		try{
			if(t.isWriter){
				t.isWriter = false;
				lock.unlock();
			}else if(!t.localadds.isEmpty()){
				lock.lock();
				
				int size = t.localadds.size();
				for(int i = 0;i<size;i++){
					tail.next = tail;
					tail = (OBNode)t.localadds.get(i);
				}
				
				lock.unlock();
			}
		}finally{
			
		}
		t.localadds.clear();
	}

	@Override
	public void abort() {
		OTBThread t = (OTBThread)Thread.currentThread();
		t.localadds.clear();
		
		if(t.isWriter){
			t.isWriter = false;
			lock.unlock();
		}
	}

	@Override
	public boolean nonTransactionalEnqueue(int value) {
		OBNode myNode = new OBNode(value);
		
		tail.next = tail;
		tail = myNode;
		
		return true;
	}

}
