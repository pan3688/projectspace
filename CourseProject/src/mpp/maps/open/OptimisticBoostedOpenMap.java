package mpp.maps.open;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import mpp.benchmarks.OpenMapThread;
import mpp.exception.AbortedException;
import mpp.maps.IntMap;


public class OptimisticBoostedOpenMap implements IntMap<Integer,Object> {

	private static final int THRESHOLD = 5;
	private static final int PROBE_SIZE = 10;
	private static final int LIMIT = 10;

	static final int PUT = 1;
	static final int GET = 2;
	static final int CONTAINS = 3;
	static final int REMOVE = 4;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock read = lock.readLock();
	private final Lock write = lock.writeLock();
	
	public volatile BucketListOpen<OBNode>[][] table;
	public AtomicInteger capacity = new AtomicInteger();
	
	@SuppressWarnings("unchecked")
	public OptimisticBoostedOpenMap(int size) {
		
		capacity.set(size);
		table = new BucketListOpen[2][capacity.get()];
		table[0][0] = new BucketListOpen<>(0);
		table[1][0] = new BucketListOpen<>(1);
	}
	
	public static int hash0(int x){
		return x;
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
	
	public boolean containsSet(Integer item) throws AbortedException {
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
		
		ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
		
		if(!postValidate(readset))
			throw AbortedException.abortedException;
		
		if(type == PUT)
			return addToTable(item, v);
		else if(type == CONTAINS)
			return containsInTable(item);
		else if(type == GET)
			return getFromTable(item);
		else
			return removeFromTable(item);
			
	}
	
	private boolean addToTable(int item, Object v){
		
		
		BucketListOpen<OBNode>[][] tableLocal = ((OpenMapThread) Thread.currentThread()).tableLocal;
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int i = -1, h = -1;
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
	
		if(containsInTable(item))
			return false;
		
		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		BucketListOpen<OBNode> b1 = getBucketList(1, h1);
		
		if(tableLocal[0][h0].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[0][h0] < THRESHOLD ){
			if(b0.add(item, hash0(item), v))
				((OpenMapThread)Thread.currentThread()).tableOps[0][h0]++;
			return true;
		}
		else if(tableLocal[1][h1].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[1][h1] < THRESHOLD ){
			if(b1.add(item, hash0(item), v))
				((OpenMapThread)Thread.currentThread()).tableOps[1][h1]++;
			return true;
		}
		else if(tableLocal[0][h0].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[0][h0] < PROBE_SIZE ){
			if(b0.add(item, hash0(item), v))
				((OpenMapThread)Thread.currentThread()).tableOps[0][h0]++;
			i = 0; h = h0;
		}
		else if(tableLocal[1][h1].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[1][h1] < PROBE_SIZE ){
			if(b1.add(item, hash0(item), v))
				((OpenMapThread)Thread.currentThread()).tableOps[1][h1]++;
			i = 1; h = h1;
		}
		else
			((OpenMapThread)Thread.currentThread()).resize = true;
		
		/*if(!relocate(i,h))
			((OpenMapThread)Thread.currentThread()).resize = true;*/
		
		return true;
	}
	
	private boolean containsInTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
		
		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		BucketListOpen<OBNode> b1 = getBucketList(1, h1);
		
		if(b0.contains(item, hash0(item)))
			return true;
		else if(b1.contains(item, hash1(item)))
			return true;
		else
			return false;
		
	}
	
	private boolean containsNonTransactional(int item){
		
		int h0 = hash0(item) % capacity.get();
		int h1 = hash1(item) % capacity.get();
		
		if(table[0][h0].containsNonTrans(item, hash0(item))){
			//System.out.println(item + " Present");
			return true;
		}
		else if(table[1][h1].containsNonTrans(item, hash1(item))){
			return true;
		}
		else
			return false;
		
	}	
	
	private Object getFromTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;
		
		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		BucketListOpen<OBNode> b1 = getBucketList(1, h1);		
		
		Object toReturn;
		
		if((toReturn = b0.get(item, hash0(item))) != null)
			return toReturn;
		else if((toReturn = b1.get(item, hash1(item))) != null)
			return toReturn;
		else
			return null;
	}
	
	private Object removeFromTable(int item){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		
		int h0 = hash0(item) % tLocalCapacity;
		int h1 = hash1(item) % tLocalCapacity;

		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		BucketListOpen<OBNode> b1 = getBucketList(1, h1);		
		
		Object toReturn;
		
		if((toReturn = b0.remove(item, hash0(item))) != null){
			((OpenMapThread)Thread.currentThread()).tableOps[0][h0]--;
			return toReturn;
		}
		else if((toReturn = b1.remove(item, hash1(item))) != null){
			((OpenMapThread)Thread.currentThread()).tableOps[1][h1]--;
			return toReturn;
		}
		else
			return false;
				
	}
	
	
	private boolean relocate(int i, int hi){
		
		int tLocalCapacity = ((OpenMapThread) Thread.currentThread()).initialCapacity;
		BucketListOpen<OBNode>[][] tableLocal = ((OpenMapThread) Thread.currentThread()).tableLocal;
		
		int hj = 0;
		int j = 1 - i;
		for(int round = 0; round < LIMIT; round++){
			
			System.out.println("i:"+i+"hi:"+hi);
			BucketListOpen<OBNode> iSet = table[i][hi];
			OBNode first;
			first = iSet.getFirst();
				
			switch(i){
			case 0: hj = hash1(first.item) % tLocalCapacity; break;
			case 1: hj = hash0(first.item) % tLocalCapacity; break;
			}
			
			BucketListOpen<OBNode> jSet = table[j][hj];
			
			if(iSet.remove(first.item, first.key) != null){
				((OpenMapThread)Thread.currentThread()).tableOps[i][hi]--;
				if(tableLocal[j][hj].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[j][hj] < THRESHOLD ){
					if(jSet.add(first.item, first.key, first.value))
						((OpenMapThread)Thread.currentThread()).tableOps[j][hj]++;
					return true;
				}
				else if(tableLocal[j][hj].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[j][hj] < PROBE_SIZE){
					if(jSet.add(first.item, first.key, first.value))
						((OpenMapThread)Thread.currentThread()).tableOps[j][hj]++;
					i = 1 - i;
					hi = hj;
					j = 1 - j;
				}
				else{
					if(iSet.add(first.item, first.key, first.value))
						((OpenMapThread)Thread.currentThread()).tableOps[i][hi]++;
					return false;
				}
			}
			else if(tableLocal[i][hi].size.get() + ((OpenMapThread)Thread.currentThread()).tableOps[i][hi] >= THRESHOLD){
				continue;
			}
			else
				return true;
		}
		
		return false;
	}
	
	private boolean postValidate(ArrayList<ReadSetEntry> readset) {
		
		if(((OpenMapThread)Thread.currentThread()).initialCapacity != capacity.get())
			return false;
		
		int initCapacity = ((OpenMapThread)Thread.currentThread()).initialCapacity;
		//check if any of the individual bucket sizes has gone over threshold
		BucketListOpen<OBNode>[][] tableLocal = ((OpenMapThread) Thread.currentThread()).tableLocal;
		for(int i = 0; i < 2; i++){
			for(int j = 0; j < initCapacity; j++){
				if(tableLocal[i][j] != null){
					if(tableLocal[i][j].size.get() != table[i][j].size.get()){
						int bucketOps = ((OpenMapThread) Thread.currentThread()).tableOps[i][j];
						if(tableLocal[i][j].size.get() + bucketOps <= THRESHOLD && table[i][j].size.get() + bucketOps <= THRESHOLD )
							tableLocal[i][j].size.set(table[i][j].size.get());
						else
							return false;
					}
				}
			}
		}
		
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
		
		//System.out.println("Lock success");
		
		if(!commitValidate(t.list_readset))
			throw AbortedException.abortedException;
		
		read.lock();
		
		//System.out.println("Started commit"+ t.getId());
		
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
		
		
		
		//Now that locks are acquired, update bucketSize for each bucket
		//If any of it goes over probe_size, set resize flag to true
		int oldSize, localOps;
		((OpenMapThread) Thread.currentThread()).resize = false;
		for(int i = 0; i < 2; i++){
			for(int j = 0; j < t.initialCapacity; j++){
				localOps = ((OpenMapThread) Thread.currentThread()).tableOps[i][j];
				while(true){
					if(table[i][j] != null){
						oldSize = table[i][j].size.get();
						if(table[i][j].size.compareAndSet(oldSize, oldSize + localOps)){
							if(table[i][j].size.get() > PROBE_SIZE)
								((OpenMapThread) Thread.currentThread()).resize = true;
							break;
						}
					}
					else
						break;
				}
			}
		}
		
		// unlock
		
		read.unlock();
		
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
		
		
		
		if(((OpenMapThread) Thread.currentThread()).resize){
			write.lock();
			if(capacity.get() == ((OpenMapThread) Thread.currentThread()).initialCapacity)
				resize();
			write.unlock();
		}
		//System.out.println("Done commit");
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		OpenMapThread t = ((OpenMapThread)Thread.currentThread());

		//System.out.println("Aborted:"+t.getId());
		
		Iterator<Entry<Integer, WriteSetEntry>> iterator = t.list_writeset.entrySet().iterator();
		WriteSetEntry entry;

		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			if(entry.pred.lockHolder == t.getId())
			{
				entry.pred.lockHolder = -1;
				entry.pred.lock.decrementAndGet();
			}

			if(entry.operation == REMOVE && entry.curr.lockHolder == t.getId())
			{
				{
					entry.curr.lockHolder = -1;
					entry.curr.lock.decrementAndGet();
				}
			}
		}

		t.list_readset.clear();
		t.list_writeset.clear();
	}

	@Override
	public boolean nonTransactionalPut(Integer item, Object v) {
		
		if(addNonTransactional(item, v)){
			System.out.println(item + "added");
			return true;
		}
		else
			return false;
	}
	
	private boolean addNonTransactional(int item, Object v){
		
		int i = -1, h = -1;
		boolean mustResize = false;
		int h0 = hash0(item) % capacity.get();
		int h1 = hash1(item) % capacity.get();
	
		BucketListOpen<OBNode> b0 = getBucketList(0, h0);	
		BucketListOpen<OBNode> b1 = getBucketList(1, h1);
		
		if(containsNonTransactional(item)){
			return false;
		}
		
		if(b0.size.get() < THRESHOLD ){
			if(b0.addNonTrans(item, hash0(item), v))
				b0.size.getAndIncrement();
			return true;
		}
		else if(b1.size.get() < THRESHOLD ){
			if(b1.addNonTrans(item, hash1(item), v))
				b1.size.getAndIncrement();
			return true;
		}
		else if(b0.size.get() < PROBE_SIZE ){
			if(b0.addNonTrans(item, hash0(item), v))
				b0.size.getAndIncrement();
			i = 0; h = h0;
		}
		else if(b1.size.get() < PROBE_SIZE ){
			if(b1.addNonTrans(item, hash1(item), v))
				b1.size.getAndIncrement();
			i = 1; h = h1;
		}
		else
			mustResize = true;
		
		if(mustResize){
			resizeNonTransactional();
			addNonTransactional(item, v);
		}
		else if(!relocateNonTransactional(i,h))
			resizeNonTransactional();
		
		return true;
	}
	
		private boolean relocateNonTransactional(int i, int hi){
		
		int hj = 0;
		int j = 1 - i;
		for(int round = 0; round < LIMIT; round++){
			BucketListOpen<OBNode> iSet = table[i][hi];
			OBNode first;
			first = iSet.getFirst();
				
			switch(i){
			case 0: hj = hash1(first.item) % capacity.get(); break;
			case 1: hj = hash0(first.item) % capacity.get(); break;
			}
			
			BucketListOpen<OBNode> jSet = getBucketList(j,hj);
			
			if(iSet.removeNonTrans(first.item, first.key) != null){
				iSet.size.getAndDecrement();
				if(jSet.size.get() < THRESHOLD ){
					if(jSet.addNonTrans(first.item, first.key, first.value))
						jSet.size.getAndIncrement();
					return true;
				}
				else if(jSet.size.get() < PROBE_SIZE){
					if(jSet.addNonTrans(first.item, first.key, first.value))
						jSet.size.getAndIncrement();
					i = 1 - i;
					hi = hj;
					j = 1 - j;
				}
				else{
					if(iSet.addNonTrans(first.item, first.key, first.value))
						iSet.size.getAndIncrement();
					return false;
				}
			}
			else if(iSet.size.get() >= THRESHOLD){
				continue;
			}
			else
				return true;
		}
		
		return false;
	}
		
		
	private void resizeNonTransactional(){
		
		int oldCapacity = capacity.get();
		capacity.set(2 * oldCapacity);
		
		System.out.println("capacity:"+capacity.get());
		
		BucketListOpen<OBNode>[][] oldTable = table;
		table = new BucketListOpen[2][capacity.get()];
		table[0][0] = new BucketListOpen<>(0);
		table[1][0] = new BucketListOpen<>(1);
		
		for(int i = 0; i < 2; i++){
			for(int j = 0; j < oldCapacity; j++){
				if(oldTable[i][j] != null){
					int size = oldTable[i][j].size.get();
					OBNode curr = oldTable[i][j].head.next;
					while(size > 0){
						addNonTransactional(curr.item, curr.value);
						size--;
					}
					
				}
			}
		}
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
		try{
			((OpenMapThread)Thread.currentThread()).tableLocal[i][parent] = table[i][parent];
			((OpenMapThread)Thread.currentThread()).tableLocal[i][myBucket] = b;

		}
		catch(ClassCastException e){
			
		}
	}
	
	private int getParent(int myBucket){
		int parent = capacity.get();
		do{
			parent = parent >> 1;
		}while(parent > myBucket);
		parent = myBucket - parent;
		return parent;
	}
	
	@SuppressWarnings("unchecked")
	private void resize(){
		int oldSize = capacity.get();
		
		BucketListOpen<OBNode>[][] oldTable = table;
		table = new BucketListOpen[2][2*oldSize];
	
		capacity.compareAndSet(oldSize, 2 * oldSize);
		
		for(int i = 0; i < 2; i++){
			for(int j = 0; j < oldSize; j++){
				table[i][j] = oldTable[i][j];
			}
		}
	}
	
	public void begin(){
		((OpenMapThread)Thread.currentThread()).tableLocal = table;
		((OpenMapThread)Thread.currentThread()).tableOps = new int[2][capacity.get()];
		((OpenMapThread)Thread.currentThread()).initialCapacity = capacity.get();
	}
}
