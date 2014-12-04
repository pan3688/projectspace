package mpp.stack;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import mpp.benchmarks.StackThread;
import mpp.exception.AbortedException;

public class OptimisticBoostedStack implements IntStack {
	
	public class OBNode {
		int item;
		public OBNode next;
		
		private OBNode(int item) {
			this.item = item;
			next = null;
		}
	}
	
	OBNode top = null;
	private final Lock lock = new ReentrantLock();
	
	public boolean push(int value) throws AbortedException{
		
		try{
			StackThread t = ((StackThread) Thread.currentThread());
			
			OBNode node = new OBNode(value);
			
			if(t.iswriter){
				node.next = top;
				top = node;
			}
			else{
				OBNode prevNode;
				int last = t.localAdds.size()-1;
				if(last < 0 )
					prevNode = null;
				else
					prevNode = t.localAdds.get(last);
				node.next = prevNode;
				t.localAdds.add(node);
			}
			
			return true;
		}
		catch(Exception e){
			throw AbortedException.abortedException;
		}
	}
	
	public int pop() throws AbortedException{
		
		try{
			StackThread t = ((StackThread) Thread.currentThread());
			OBNode toReturn;
			
			if(!t.localAdds.isEmpty()){
				int last = t.localAdds.size()-1;
				toReturn = t.localAdds.get(last);
				t.localAdds.remove(last);
				return toReturn.item;
			}
			else{
				lock.lock();
				t.iswriter = true;
			}
			
			if(t.iswriter){
				toReturn = top;
				if(toReturn == null)
					return Integer.MIN_VALUE;
				top = top.next;
				return toReturn.item;
			}
			
			
		}
		catch(Exception e){
			throw AbortedException.abortedException;
		}
		
		return Integer.MIN_VALUE;
	}
	
	
	public int top() throws AbortedException{
		
		if(top == null)
			return Integer.MIN_VALUE;
		else
			return top.item;
	}
	
	public void commit() throws AbortedException{
	
		StackThread t = ((StackThread) Thread.currentThread());
		if (t.iswriter) {
			t.iswriter = false;
			lock.unlock();
		} 
		else if (!t.localAdds.isEmpty()) {
			lock.lock();

			// publish adds
			int last = t.localAdds.size()-1;
			
			OBNode localTop = t.localAdds.get(last);
			OBNode end = t.localAdds.get(0);
			
			end.next = top;
			top = localTop;
			
				
			lock.unlock();
		}
		t.localAdds.clear();
	}
	
	public void abort(){
		
		StackThread t = ((StackThread) Thread.currentThread());
		t.localAdds.clear();
		if(t.iswriter)
		{
			t.iswriter = false;
			lock.unlock();
		}
	}

	public boolean nonTransactionalPush(int value){
		
		OBNode node = new OBNode(value);
		node.next = top;
		top = node;
		
		return true;
	}

}
