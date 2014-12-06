package mpp.maps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import mpp.benchmarks.OpenMapThread;
import mpp.exception.AbortedException;

public class OptimisticBoostedOpenMap implements IntMap<Integer,Object> {

	private static final int THRESHOLD = 3;
	private static final int PROBE_SIZE = 5;

	final int PUT = 1;
	final int GET = 2;
	final int CONTAINS = 3;
	final int REMOVE = 4;
	
	public volatile BucketListOpen<OBNode>[][] table;
	public AtomicInteger capacity;
	
	@SuppressWarnings("unchecked")
	public OptimisticBoostedOpenMap(int size) {
		
		capacity.set(size);
		table = new BucketListOpen[2][capacity.get()];
		table[0][0] = new BucketListOpen<>(0);
		table[1][0] = new BucketListOpen<>(1);
	}
	
	public int hash0(int x){
		return (x % 15) % capacity.get();
	}
	
	public int hash1(int x){
		return (x % 11) % capacity.get();
	}
	
	
	public class ReadSetEntry {
		OBNode pred;
		OBNode curr;
		boolean checkLink;
		
		public ReadSetEntry(OBNode pred, OBNode curr, boolean checkLink) {
			this.pred = pred;
			this.curr = curr;
			this.checkLink = checkLink;
		}	
	}
	
	public class WriteSetEntry {
		int item;
		OBNode pred;
		OBNode curr;
		OBNode newNode;
		int operation;
		int key;
		Object value;
		
		public WriteSetEntry(OBNode pred, OBNode curr, int operation, int key, int item, Object value) {
			this.pred = pred;
			this.curr = curr;
			this.operation = operation;
			this.key = key;
			this.item = item;
			this.value = value;
		}
	}
	
	public boolean put(Integer item, Object v) throws AbortedException{
		return (boolean)operation(PUT, item);
	}
	
	public Object remove(Integer item) throws AbortedException {
		return operation(REMOVE, item);
	}
	
	public Object get(Integer item) throws AbortedException {
		return operation(GET, item);
	}
	
	public boolean contains(Integer item) throws AbortedException {
		return (boolean)operation(CONTAINS, item);
	}
	
	private boolean operation(int type, int item) throws AbortedException{
		
		TreeMap<Integer, WriteSetEntry> writeset = ((OpenMapThread) Thread.currentThread()).list_writeset;
		
		WriteSetEntry entry = writeset.get(item);
		
		if(entry != null){
			
			if(entry.operation == PUT){
				if(type == PUT)
					return false;
				else if(type == CONTAINS || type == GET)
					return true;
				else{
					writeset.remove(item);
					return true;
				}
				
			}else{
				if(type == REMOVE || type == CONTAINS)
					return false;
				else{
					writeset.remove(myNode.key);
					return true;
				}
			}
		}
		
		ArrayList<ReadSetEntry> readset = ((ClosedMapThread) Thread.currentThread()).list_readset;
		
		// this is where it differs from actual set
		// will help provide constant time operations
		int myBucket = new Integer(myNode.key).hashCode() % bucketSize.get();
		BucketList<OBNode> b = getBucketList(myBucket);						
		
		Window window = b.find(b.head, myNode.key);
		
		OBNode pred = window.pred;
		OBNode curr = window.curr;
		
		int currKey = curr.key;
		boolean currMarked = curr.marked;
		
		if(!postValidate(readset))
			throw AbortedException.abortedException;
		
		if(currKey == myNode.key && !currMarked){
			if(type == CONTAINS){
				readset.add(new ReadSetEntry(pred, curr, false));
				return true;
			}else if(type == PUT){
				readset.add(new ReadSetEntry(pred, curr, false));
				return false;
			}else{
				readset.add(new ReadSetEntry(pred, curr, true));
				writeset.put(myNode.key, new WriteSetEntry(pred, curr, REMOVE, myNode.key,myNode.value));
				return true;
			}
		}else{
			readset.add(new ReadSetEntry(pred, curr, true));
			if(type == CONTAINS || type == REMOVE)
				return false;
			else{
				writeset.put(myNode.key, new WriteSetEntry(pred, curr, PUT, myNode.key,myNode.value));
				return true;
			}
		}
	}
	
	private boolean postValidate(ArrayList<ReadSetEntry> readset) {
		ReadSetEntry entry;	
		int size = readset.size();

		int [] predLocks = new int[size];
		int [] currLocks = new int[size];
		
		// get snapshot of lock values
		for(int i=0; i<size; i++)
		{
			entry = readset.get(i);
			predLocks[i] = entry.pred.lock.get();
			currLocks[i] = entry.curr.lock.get();
		}
		
		// check the values of the nodes 
		// and also check that nodes are not currently locked
		for(int i=0; i<size; i++)
		{
			entry = readset.get(i);
			if((currLocks[i] & 1) == 1 || entry.curr.marked)
				return false;
			if(entry.checkLink)
			{
				if((predLocks[i] & 1) == 1 || entry.pred.marked || entry.curr != entry.pred.next.getReference()) 
					return false;
			}
		}
		
		// check that lock values are still the same since validation starts
		for(int i=0; i<size; i++)
		{
			entry = readset.get(i);
			if(currLocks[i] != entry.curr.lock.get())
				return false;
			if(entry.checkLink && predLocks[i] != entry.pred.lock.get())
				return false;
		}
		return true;
	}
	
	private boolean commitValidate(ArrayList<ReadSetEntry> readset){
		
		ReadSetEntry entry;	
		int size = readset.size();

		// check the values of the nodes 
		for(int i=0; i<size; i++)
		{
			entry = readset.get(i);
			if(entry.curr.marked)
				return false;
			if(entry.checkLink && (entry.pred.marked || entry.curr != entry.pred.next.getReference()))
				return false;
		}
		return true;
		
	}

	@Override
	public void commit() throws AbortedException {
		// TODO Auto-generated method stub
		ClosedMapThread t = ((ClosedMapThread) Thread.currentThread());
		
		Set<Entry<Integer, WriteSetEntry>> write_set = t.list_writeset.entrySet();
		ArrayList<ReadSetEntry> read_set = t.list_readset;
		
		// read-only transactions do nothing
		if(write_set.isEmpty())
		{
			read_set.clear();
			return;
		}
		
		long threadId = Thread.currentThread().getId();
		Iterator<Entry<Integer, WriteSetEntry>> iterator = write_set.iterator();
		WriteSetEntry entry;
		
		int predLock, currLock;
		OBNode newNodeOrVictim;
		
		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			
			predLock = entry.pred.lock.get();
			currLock = entry.curr.lock.get();
			
			if((predLock & 1) == 1 && entry.pred.lockHolder != threadId)
				throw AbortedException.abortedException;
			// if operation is REMOVE, check that curr lock is not acquired by another thread
			if(entry.operation == REMOVE && (currLock & 1) == 1 && entry.curr.lockHolder != threadId)
				throw AbortedException.abortedException;
			
			// Try to acquire pred lock
			if(entry.pred.lockHolder == threadId || entry.pred.lock.compareAndSet(predLock, predLock + 1)){
							// if operation is REMOVE, try to acquire curr lock
				entry.pred.lockHolder = threadId;
				
				if(entry.operation == REMOVE){
					if(entry.curr.lockHolder == threadId || entry.curr.lock.compareAndSet(currLock, currLock + 1))
						entry.curr.lockHolder = threadId;
						// in case of failure, unlock pred and abort.
					else{
						entry.pred.lockHolder = -1;
						entry.pred.lock.decrementAndGet();
						throw AbortedException.abortedException;
					}
				}
			}else
				throw AbortedException.abortedException;
		}
		
		if(!commitValidate(t.list_readset))
			throw AbortedException.abortedException;

		// Publish write-set
		iterator = write_set.iterator();
		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			
			
			OBNode pred = entry.pred;
			OBNode curr = entry.pred.next.getReference();
			while(curr.key < entry.key)
			{
				pred = curr;
				curr = curr.next.getReference();
			}
			
			if(entry.operation == PUT)
			{
				newNodeOrVictim = new OBNode(entry.key,entry.value);
				newNodeOrVictim.lock.set(1);
				newNodeOrVictim.lockHolder = threadId;
				entry.newNode = newNodeOrVictim;
				newNodeOrVictim.next.set(curr,false);
				pred.next.set(newNodeOrVictim,false);
			}
			else // remove
			{
				curr.marked = true;
				pred.next = entry.curr.next;
			}			
		}
		
		// unlock
		iterator = write_set.iterator();
		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			if(entry.pred.lockHolder == threadId)
			{
				entry.pred.lockHolder = -1;
				entry.pred.lock.incrementAndGet();
			}
			// newNodeOrVictim in this case is either the added or the removed node 
			if(entry.operation == REMOVE)
				newNodeOrVictim = entry.curr;
			else // add
				newNodeOrVictim = entry.newNode;
			if (newNodeOrVictim.lockHolder == threadId) {
				newNodeOrVictim.lockHolder = -1;
				newNodeOrVictim.lock.incrementAndGet();
			}
		}
		
		// clear read- and write- sets
		read_set.clear();
		write_set.clear();
	}

	@Override
	public boolean abort() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nonTransactionalPut(Integer k, Object v) {
		// TODO Auto-generated method stub
		return false;
	}

	private BucketList<OBNode> getBucketList(int myBucket){
		if(bucket[myBucket] == null)
			initializeBucket(myBucket);
		
		return bucket[myBucket];
	}
	
	private void initializeBucket(int myBucket){
		int parent = getParent(myBucket);
		if(bucket[parent] == null)
			initializeBucket(parent);
		BucketList<OBNode> b = bucket[parent].getSentinel(myBucket);
		if(b != null)
			bucket[myBucket] = b;
	}
	
	private int getParent(int myBucket){
		int parent = bucketSize.get();
		do{
			parent = parent >> 1;
		}while(parent > myBucket);
		parent = myBucket - parent;
		return parent;
	}
}
