import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;


public class OptimisticBoostedList implements IntSet{
	
	final int ADD = 0;
	final int REMOVE = 1;
	final int CONTAINS = 2;
	
	int lockAbort = 0;
	int validateAbort = 0;
	
	class OBNode {
		int item;
		volatile boolean marked;
		public OBNode next;
		volatile AtomicInteger lock = new AtomicInteger(0);
		volatile long lockHolder = -1;

		private OBNode(int item) {
			this.item = item;
			marked = false;
		}
	}
	
	class ReadSetEntry {
		OBNode pred;
		OBNode curr;
		boolean checkLink;
		
		public ReadSetEntry(OBNode pred, OBNode curr, boolean checkLink) {
			this.pred = pred;
			this.curr = curr;
			this.checkLink = checkLink;
		}	
	}
	
	class WriteSetEntry {
		int item;
		OBNode pred;
		OBNode curr;
		OBNode newNode;
		int operation;
		
		public WriteSetEntry(OBNode pred, OBNode curr, int operation, int item) {
			this.pred = pred;
			this.curr = curr;
			this.operation = operation;
			this.item = item;
		}
	}
	
	OBNode head = new OBNode(Integer.MIN_VALUE);
	OBNode tail = new OBNode(Integer.MAX_VALUE);

	public OptimisticBoostedList() {
		head.next = tail;
		tail.next = tail;
	}

	@Override
	public boolean add(int item) throws AbortedException {
		return operation(ADD, item);
	}

	@Override
	public boolean remove(int item) throws AbortedException {
		return operation(REMOVE, item);
	}

	@Override
	public boolean contains(int item) throws AbortedException {
		return operation(CONTAINS, item);
	}

	private boolean commitValidate(ArrayList<ReadSetEntry> readset) {
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
	
	private boolean operation(int type, int item) throws AbortedException
	{
		TreeMap<Integer, WriteSetEntry> writeset = ((SetThread) Thread.currentThread()).list_writeset;
		
		WriteSetEntry entry = writeset.get(item);
		if(entry != null)
		{
			if(entry.operation == ADD)
			{
				if(type == ADD) return false;
				else if(type == CONTAINS) return true;
				else // remove
				{
//					System.out.println("remove after add");
					writeset.remove(item);
					return true;
				}
			}
			else // remove
			{
				if(type == REMOVE || type == CONTAINS)
					return false;
				else // add
				{
//					System.out.println("add after remove");
					writeset.remove(item);
					return true;
				}
			}
		}
	
		ArrayList<ReadSetEntry> readset = ((SetThread) Thread.currentThread()).list_readset;
		
		OBNode pred = head, curr = head.next;
		while (curr.item < item)
		{
			pred = curr;
			curr = curr.next;
		}
		
		int currItem = curr.item;
		boolean currMarked = curr.marked;
		
		if(!postValidate(readset))
			throw AbortedException.abortedException;
		
		if(currItem == item && !currMarked)
		{
			
			if(type == CONTAINS)
			{
				readset.add(new ReadSetEntry(pred, curr, false));
				return true;
			}
			else if(type == ADD)
			{
				readset.add(new ReadSetEntry(pred, curr, false));
				return false;
			}
			else // remove
			{
				readset.add(new ReadSetEntry(pred, curr, true));
				writeset.put(item, new WriteSetEntry(pred, curr, REMOVE, item));
				return true;
			}
		}
		else
		{
			readset.add(new ReadSetEntry(pred, curr, true));
			if(type == CONTAINS || type == REMOVE)
				return false;
			else // add
			{
				writeset.put(item, new WriteSetEntry(pred, curr, ADD, item));
				return true;
			}
		}
	}

	@Override
	public void commit() throws AbortedException {
		
		SetThread t = ((SetThread) Thread.currentThread());
		
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
		
		// Acquire locks
		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			
			predLock = entry.pred.lock.get();
			currLock = entry.curr.lock.get();			
			
			// check that pred lock is not acquired by another thread
			if((predLock & 1) == 1 && entry.pred.lockHolder != threadId)
				throw AbortedException.abortedException;
			// if operation is REMOVE, check that curr lock is not acquired by another thread
			if(entry.operation == REMOVE && (currLock & 1) == 1 && entry.curr.lockHolder != threadId)
				throw AbortedException.abortedException;
			
			// Try to acquire pred lock
			if(entry.pred.lockHolder == threadId || entry.pred.lock.compareAndSet(predLock, predLock + 1))
			{
				// if operation is REMOVE, try to acquire curr lock
				entry.pred.lockHolder = threadId;
				if(entry.operation == REMOVE)
				{
					if(entry.curr.lockHolder == threadId || entry.curr.lock.compareAndSet(currLock, currLock + 1))
						entry.curr.lockHolder = threadId;
					// in case of failure, unlock pred and abort.
					else
					{
						entry.pred.lockHolder = -1;
						entry.pred.lock.decrementAndGet();
						throw AbortedException.abortedException;
					}
				}
			}
			else
				throw AbortedException.abortedException;
		}
		
		// validate read-set
		if(!commitValidate(t.list_readset))
			throw AbortedException.abortedException;

		// Publish write-set
		iterator = write_set.iterator();
		while(iterator.hasNext())
		{
			entry = iterator.next().getValue();
			
			
			OBNode pred = entry.pred;
			OBNode curr = entry.pred.next;
			while(curr.item < entry.item)
			{
				pred = curr;
				curr = curr.next;
			}
			
			if(entry.operation == ADD)
			{
				newNodeOrVictim = new OBNode(entry.item);
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
		
		SetThread t = ((SetThread) Thread.currentThread());
		
		// internally you must not call this method. It's sufficient to throw AbortException
		// This method will be called from calling transaction as a part of its abort subroutine
		// This way: Abort is handled by transaction not by data structure.
		
		// Unlock
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
		
		// clear read- and write- sets
		t.list_readset.clear();
		t.list_writeset.clear();
		
	}
	
	public boolean nontransactionalAdd(int item)
	{
		OBNode pred = head, curr = head.next;
		while (curr.item < item)
		{
			pred = curr;
			curr = curr.next;
		}
		if(curr.item == item)
			return false;
		else
		{
			OBNode node = new OBNode(item);
			node.next = curr;
			pred.next = node;
			return true;
		}
			
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
