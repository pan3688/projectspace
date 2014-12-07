package mpp.maps.open;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import mpp.benchmarks.OpenMapThread;
import mpp.exception.AbortedException;
import mpp.maps.IntMap;
import mpp.maps.Window;

public class OptimisticBoostedOpenMap implements IntMap<Integer,Object> {

	private static final int THRESHOLD = 3;
	private static final int PROBE_SIZE = 5;
	private static final int LIMIT = 10;

	static final int PUT = 1;
	static final int GET = 2;
	static final int CONTAINS = 3;
	static final int REMOVE = 4;
	
	public volatile BucketListOpen<OBNode>[][] table;
	public AtomicInteger capacity;
	
	@SuppressWarnings("unchecked")
	public OptimisticBoostedOpenMap(int size) {
		
		capacity.set(size);
		table = new BucketListOpen[2][capacity.get()];
		table[0][0] = new BucketListOpen<>(0);
		table[1][0] = new BucketListOpen<>(1);
	}
	
	public static int hash0(int x){
		return (x % 15);
	}
	
	public static int hash1(int x){
		return (x % 11);
	}
	
	
	public static class ReadSetEntry {
		OBNode pred;
		OBNode curr;
		boolean checkLink;
		
		public ReadSetEntry(OBNode pred, OBNode curr, boolean checkLink) {
			this.pred = pred;
			this.curr = curr;
			this.checkLink = checkLink;
		}	
	}
	
	public static class WriteSetEntry {
		int item;
		OBNode pred;
		OBNode curr;
		OBNode newNode;
		int operation;
		int key;
		Object value;
		int parentHID;
		
		public WriteSetEntry(OBNode pred, OBNode curr, int operation, int key, int item, Object value, int pHID) {
			this.pred = pred;
			this.curr = curr;
			this.operation = operation;
			this.key = key;
			this.item = item;
			this.value = value;
			this.parentHID = pHID;
		}
	}
	
	public boolean put(Integer item, Object v) throws AbortedException{
		return (boolean)operation(PUT, item, v);
	}
	
	public Object remove(Integer item) throws AbortedException {
		return operation(REMOVE, item, null);
	}
	
	public Object get(Integer item) throws AbortedException {
		return operation(GET, item, null);
	}
	
	public boolean contains(Integer item) throws AbortedException {
		return (boolean)operation(CONTAINS, item, null);
	}
	
	private Object operation(int type, int item, Object v) throws AbortedException{
		
		TreeMap<Integer, WriteSetEntry> writeset = ((OpenMapThread) Thread.currentThread()).list_writeset;
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		WriteSetEntry entry = writeset.get(item);
		
		if(entry != null){
			
			if(entry.operation == PUT){
				if(type == PUT)
					return false;
				else if(type == CONTAINS)
					return true;
				else if(type == GET)
					return entry.value;
				else{
					writeset.remove(item);
					((OpenMapThread)Thread.currentThread()).tableOps[entry.parentHID][entry.key % tLocalCapacity]--;
					return true;
				}
				
			}else{ //remove
				
				if( type == CONTAINS)
					return false;
				else if(type == REMOVE || type == GET)
					return null;
				else{
					//add after remove
					writeset.remove(item);
					((OpenMapThread)Thread.currentThread()).tableOps[entry.parentHID][entry.key % tLocalCapacity]--;
					return true;
				}
			}
		}
		
		
		if(type == PUT)
			return addToTable(item, v);
		else if(type == CONTAINS)
			return containsInTable(item);
		else if(type == GET)
			return getFromTable(item);
		else if(type == REMOVE)
			return removeFromTable(item);
		
		
		ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
		
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
	
	private boolean addToTable(int item, Object v){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
	
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
	
		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		
		return true;
	}
	
	private boolean containsInTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
		
		if(table[0][h0].contains(item, hash0(item)))
			return true;
		else if(table[1][h1].contains(item, hash1(item)))
			return true;
		else
			return false;
		
	}
	
	private Object getFromTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
		Object toReturn;
		
		if((toReturn = table[0][h0].get(item, hash0(item))) != null)
			return toReturn;
		else if((toReturn = table[1][h1].get(item, hash1(item))) != null)
			return toReturn;
		else
			return null;
	}
	
	private Object removeFromTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
		Object toReturn;
		
		if((toReturn = table[0][h0].remove(item, hash0(item))) != null){
			((OpenMapThread)Thread.currentThread()).tableOps[0][h0]--;
			return toReturn;
		}
		else if((toReturn = table[1][h1].remove(item, hash1(item))) != null){
			((OpenMapThread)Thread.currentThread()).tableOps[1][h1]--;
			return toReturn;
		}
		else
			return false;
				
	}
	
	
	private boolean relocate(int i, int hi){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int hj = 0;
		int j = i-1;
		for(int round = 0; round < LIMIT; round++){
			BucketListOpen<OBNode> iSet = table[i][hi];
			OBNode first;
			if((first = iSet.getFirst()) != null)
				((OpenMapThread)Thread.currentThread()).tableOps[i][hi]--;
			
			switch(i){
			case 0: hj = hash1(first.item) % tLocalCapacity; break;
			case 1: hj = hash0(first.item) % tLocalCapacity; break;
			}
			
			BucketListOpen<OBNode> jSet = table[j][hj];
			
			if(iSet.remove(first.item, first.key) != null){
				
			}
			else if(iSet.size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[i][hi] >= THRESHOLD){
				continue;
			}
			else
				return true;
			
			
			
			
		}
		
		return false;
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
				if((predLocks[i] & 1) == 1 || entry.pred.marked || entry.curr != entry.pred.next) 
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
			if(entry.checkLink && (entry.pred.marked || entry.curr != entry.pred.next))
				return false;
		}
		return true;
		
	}

	@Override
	public void commit() throws AbortedException {
		// TODO Auto-generated method stub
		OpenMapThread t = ((OpenMapThread) Thread.currentThread());
		
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
			OBNode curr = entry.pred.next;
			while(curr.key < entry.key)
			{
				pred = curr;
				curr = curr.next;
			}
			
			if(entry.operation == PUT)
			{
				newNodeOrVictim = new OBNode(entry.key, entry.item, entry.value);
				newNodeOrVictim.lock.set(1);
				newNodeOrVictim.lockHolder = threadId;
				entry.newNode = newNodeOrVictim;
				newNodeOrVictim.next = curr;
				pred.next = newNodeOrVictim;
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
	public void abort() {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean nonTransactionalPut(Integer k, Object v) {
		// TODO Auto-generated method stub
		return false;
	}

	private BucketListOpen<OBNode> getBucketList(int i, int myBucket){
		if(table[i][myBucket] == null)
			initializeBucket(i, myBucket);
		
		return table[i][myBucket];
	}
	
	private void initializeBucket(int i, int myBucket){
		int parent = getParent(myBucket);
		if(table[i][parent] == null)
			initializeBucket(i,parent);
		BucketListOpen<OBNode> b = table[i][parent].getSentinel(i, myBucket);
		if(b != null)
			table[i][myBucket] = b;
	}
	
	private int getParent(int myBucket){
		int parent = capacity.get();
		do{
			parent = parent >> 1;
		}while(parent > myBucket);
		parent = myBucket - parent;
		return parent;
	}
	
	public void begin(){
		((OpenMapThread)Thread.currentThread()).tableLocal = table;
		((OpenMapThread)Thread.currentThread()).tableOps = new int[2][capacity.get()];
		((OpenMapThread)Thread.currentThread()).initialCapacity = capacity.get();
	}
}
