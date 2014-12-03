package mpp.queue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import mpp.benchmarks.QueueThread;
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
	OBNode head = new OBNode(-1);
	OBNode tail = head;
	
	@Override
	public boolean add(int value) throws AbortedException {
		OBNode mynode = new OBNode(value);
		
		QueueThread t = ((QueueThread)Thread.currentThread());
		
		try{
			if(t.isWriter){
				tail.next = mynode;
				tail = mynode;
			}else{
				if(t.localadds.size() < 1)
					t.localadds.add(mynode);
				else{
					int size = t.localadds.size();
					t.localadds.get(size-1).next = mynode;
					t.localadds.add(mynode);
				}
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
			QueueThread t = (QueueThread)Thread.currentThread();
			
			if(!t.isWriter){
				lock.lock();
				
				t.isWriter = true;
				
				int size = t.localadds.size();
				/*for(int i = 0;i<size;i++){
					tail.next = tail;
					tail = (OBNode)t.localadds.get(i);
				}*/
				if(size > 0){
					OBNode localHead = t.localadds.get(0);
					OBNode localTail = t.localadds.get(size - 1);
					tail.next = localHead;
					tail = localTail;
				}
			}
			myNode = head.next;
			head.next = myNode.next;
		}catch(Exception e){
			throw AbortedException.abortedException;
		}
		
		return myNode.item;
	}
	@Override
	public int element() throws AbortedException {
		return head.next.item;
	}

	@Override
	public void commit() throws AbortedException {
		QueueThread t = (QueueThread)Thread.currentThread();
		
		try{
			if(t.isWriter){
				t.isWriter = false;
				lock.unlock();
			}else if(!t.localadds.isEmpty()){
				
				int size = t.localadds.size();
				
				OBNode localHead = t.localadds.get(0);
				OBNode localTail = t.localadds.get(size - 1);
				/*for(int i=0;i<size-1;i++){
					((OBNode)t.localadds.get(i)).next = (OBNode)t.localadds.get(i+1);
				}*/
				lock.lock();
				
				tail.next = localHead;
				
				tail = localTail;
				
				lock.unlock();
			}
		}finally{
			
		}
		t.localadds.clear();
	}

	@Override
	public void abort() {
		QueueThread t = (QueueThread)Thread.currentThread();
		t.localadds.clear();
		
		if(t.isWriter){
			t.isWriter = false;
			lock.unlock();
		}
	}

	@Override
	public boolean nonTransactionalEnqueue(int value) {
		OBNode myNode = new OBNode(value);
		
		tail.next = myNode;
		tail = myNode;
		
		return true;
	}

}
