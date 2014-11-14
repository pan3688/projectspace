public class OptimisticBoostedSkipPQ implements IntPQ {
	
	OptimisticBoostedPrioritySkipList skipList = new OptimisticBoostedPrioritySkipList();
	
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
		t.OBLocalAdds.clear();
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
