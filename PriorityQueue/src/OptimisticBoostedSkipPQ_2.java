import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OptimisticBoostedSkipPQ_2 implements IntPQ {
	
	OptimisticBoostedPrioritySkipList skipList = new OptimisticBoostedPrioritySkipList();
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock read = lock.readLock();
	private final Lock write = lock.writeLock();
	
	@Override
	public boolean add(int value) throws AbortedException {
		PQThread t = ((PQThread) Thread.currentThread());
		if(skipList.add(value))
		{
			t.OBLocalAdds.add(value);
			return true;
		}
		else
			return false;
	}

	@Override
	public int removeMin() throws AbortedException {
		PQThread t = ((PQThread) Thread.currentThread());
		OptimisticBoostedPrioritySkipList.OBNode minPred, min;
		
		if (!t.iswriter) {
			if (t.isReader) {
				read.unlock();
				t.isReader = false;
			}
			else // neither reader nor writer
				t.firstMin = skipList.head.next[0];
			write.lock();
			t.iswriter = true;
		}

		min = t.lastRemovedMin.next[0];
		minPred = t.lastRemovedMin;
		
		
		boolean minFromLocalAdds = false;
		Integer localMin = t.OBLocalAdds.peek();
		if(localMin != null && localMin < min.item)
			minFromLocalAdds = true;
		
		if(minFromLocalAdds && !skipList.contains(min.item))
			throw AbortedException.abortedException;
		
		if(!minFromLocalAdds && !skipList.remove(min.item))
			throw AbortedException.abortedException;
		
		if(minPred.next[0] != min)
			throw AbortedException.abortedException;
		
		if(minFromLocalAdds)
		{
			localMin = t.OBLocalAdds.poll();
			return localMin;
		}
		else
		{
			t.lastRemovedMin = min;
			return min.item;
		}
	}

	@Override
	public int getMin() throws AbortedException {
		PQThread t = ((PQThread) Thread.currentThread());
		OptimisticBoostedPrioritySkipList.OBNode minPred, min;
		
		if (!t.iswriter && !t.isReader) {
			read.lock();
			t.isReader = true;
			t.firstMin = skipList.head.next[0];
		}
		
		min = t.lastRemovedMin.next[0];
		minPred = t.lastRemovedMin;
		
		if(!skipList.contains(min.item))
			throw AbortedException.abortedException;
		
		if(minPred.next[0] != min)
			throw AbortedException.abortedException;

		// return local min if its smaller than global min
		Integer localMin = t.OBLocalAdds.peek();
		if(localMin != null && localMin < min.item)
			return localMin;
		
		return min.item;
	}

	@Override
	public void commit() throws AbortedException {

		skipList.commit();
		
		// unlock and reset PQ variables
		PQThread t = ((PQThread) Thread.currentThread());
		t.firstMin = null;
		t.lastRemovedMin = skipList.head;
		unlockAll(t);
		t.OBLocalAdds.clear();
	}

	@Override
	// Probably will not be called in our benchmark because we are using
	// unbounded
	// priority queue, and we are not putting timeouts for locks
	// The only case of calling is when exception occurs during locking
	public void abort() {

		skipList.abort();
		
		// unlock and reset PQ variables
		PQThread t = ((PQThread) Thread.currentThread());
		t.firstMin = null;
		t.lastRemovedMin = skipList.head;
		unlockAll(t);
		t.OBLocalAdds.clear();
	}
	
	private void unlockAll(PQThread t) {
		if (t.iswriter) {
			write.unlock();
			t.iswriter = false;
		}
		if (t.isReader) {
			read.unlock();
			t.isReader = false;
		}
	}

	@Override
	public boolean nontransactionalAdd(int item) {
		return skipList.nontransactionalAdd(item);
	}

	@Override
	public void begin() {
		PQThread t = ((PQThread) Thread.currentThread());
		t.lastRemovedMin = skipList.head;
		t.firstMin = null;
	}

}
