import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OptimisticBoostedSkipList implements IntSet {

	static final int MAX_LEVEL = 31;

	final int ADD = 0;
	final int REMOVE = 1;
	final int CONTAINS = 2;

	private static final class OBNode {
		volatile AtomicInteger lock = new AtomicInteger(0);
		volatile long lockHolder = -1;

		final int item;
		final OBNode[] next;
		volatile boolean marked = false;
		volatile boolean fullyLinked = false;
		private int topLevel;

		public OBNode(int item) { // sentinel node constructor
			this.item = item;
			next = new OBNode[MAX_LEVEL + 1];
			topLevel = MAX_LEVEL;
		}

		public OBNode(int item, int height) {
			this.item = item;
			next = new OBNode[height + 1];
			topLevel = height;
		}

		// public void lock() {
		// lock.lock();
		// }
		//
		// public void unlock() {
		// lock.unlock();
		// }
	}

	final OBNode head = new OBNode(Integer.MIN_VALUE);
	final OBNode tail = new OBNode(Integer.MAX_VALUE);

	private static final Random seedGenerator = new Random();
	private transient int randomSeed;

	class ReadSetEntry {
		OBNode[] preds;
		OBNode[] currs;
		int checkLink;
		int topLevel;

		public ReadSetEntry(OBNode[] pred, OBNode[] curr, int checkLink,
				int topLevel) {
			this.preds = pred;
			this.currs = curr;
			this.checkLink = checkLink;
			this.topLevel = topLevel;
		}
	}

	class WriteSetEntry {
		int item;
		OBNode[] preds;
		OBNode[] currs;
		OBNode newNode; // used only at commit
		int operation;
		int topLevel;

		public WriteSetEntry(OBNode[] pred, OBNode[] curr, int operation,
				int item, int topLevel) {
			this.preds = pred;
			this.currs = curr;
			this.operation = operation;
			this.item = item;
			this.topLevel = topLevel;
		}
	}

	public OptimisticBoostedSkipList() {
		randomSeed = seedGenerator.nextInt() | 0x0100; // ensure nonzero
		for (int i = 0; i < head.next.length; i++) {
			head.next[i] = tail;
		}
	}

	private int randomLevel() {
		int x = randomSeed;
		x ^= x << 13;
		x ^= x >>> 17;
		randomSeed = x ^= x << 5;
		if ((x & 0x8001) != 0) // test highest and lowest bits
			return 0;
		int level = 1;
		while (((x >>>= 1) & 1) != 0)
			++level;
		return level;
	}

	int find(int item, OBNode[] preds, OBNode[] succs) {

		int lFound = -1;
		OBNode pred = head;
		for (int level = MAX_LEVEL; level >= 0; level--) {
			OBNode curr = pred.next[level];
			while (item > curr.item) {
				pred = curr;
				curr = pred.next[level];
			}
			if (lFound == -1 && item == curr.item) {
				lFound = level;
			}
			preds[level] = pred;
			succs[level] = curr;
		}
		return lFound;
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

		for (int i = 0; i < size; i++) {
			entry = readset.get(i);
			int topLevel = entry.topLevel;
			if (entry.checkLink == 0) {
				if (entry.currs[topLevel].marked)
					return false;
			} else if (entry.checkLink == 1) {
				OBNode pred = entry.preds[0];
				OBNode curr = entry.currs[0];
				if (pred.marked || curr.marked || curr != pred.next[0])
					return false;
			} else {
				for (int level = 0; level <= topLevel; level++) {
					OBNode pred = entry.preds[level];
					OBNode curr = entry.currs[level];
					if (pred.marked || curr.marked || curr != pred.next[level])
						return false;
				}
			}
		}
		return true;
	}

	private boolean postValidate(ArrayList<ReadSetEntry> readset) {
		ReadSetEntry entry;
		int size = readset.size();
		int[][] predLocks = new int[size][MAX_LEVEL];
		int[][] currLocks = new int[size][MAX_LEVEL];

		// get snapshot of lock values
		for (int i = 0; i < size; i++) {
			entry = readset.get(i);
			int topLevel = entry.topLevel;
			if (entry.checkLink == 0)
				currLocks[i][topLevel] = entry.currs[topLevel].lock.get();
			else if (entry.checkLink == 1) {
				predLocks[i][0] = entry.preds[0].lock.get();
				currLocks[i][0] = entry.currs[0].lock.get();
			} else {
				for (int level = 0; level <= topLevel; level++) {
					predLocks[i][level] = entry.preds[level].lock.get();
					currLocks[i][level] = entry.currs[level].lock.get();
				}
			}
		}

		// check the values of the nodes
		// and also check that nodes are not currently locked
		for (int i = 0; i < size; i++) {
			entry = readset.get(i);
			int topLevel = entry.topLevel;
			if (entry.checkLink == 0) {
				if ((currLocks[i][topLevel] & 1) == 1
						|| entry.currs[topLevel].marked)
					return false;
			} else if (entry.checkLink == 1) {
				OBNode pred = entry.preds[0];
				OBNode curr = entry.currs[0];
				if ((currLocks[i][0] & 1) == 1 || (predLocks[i][0] & 1) == 1
						|| pred.marked || curr.marked || curr != pred.next[0])
					return false;
			} else {
				for (int level = 0; level <= topLevel; level++) {
					OBNode pred = entry.preds[level];
					OBNode curr = entry.currs[level];
					if ((currLocks[i][level] & 1) == 1
							|| (predLocks[i][level] & 1) == 1 || pred.marked
							|| curr.marked || curr != pred.next[level])
						return false;
				}
			}
		}
		
		// check that lock values are still the same since validation starts
		for(int i=0; i<size; i++)
		{
			entry = readset.get(i);
			int topLevel = entry.topLevel;
			if(entry.checkLink == 0)
				if(currLocks[i][topLevel] != entry.currs[topLevel].lock.get())
					return false;
			else if(entry.checkLink == 1)
				if(predLocks[i][0] != entry.preds[0].lock.get() || currLocks[i][0] != entry.currs[0].lock.get())
					return false;
			else
			{
				for (int level = 0; level <= topLevel; level++) {
					if(predLocks[i][level] != entry.preds[level].lock.get() || currLocks[i][level] != entry.currs[level].lock.get())
						return false;
				}
			}
		}
		
		return true;
	}

	private boolean operation(int type, int item) throws AbortedException {
		TreeMap<Integer, WriteSetEntry> writeset = ((SetThread) Thread
				.currentThread()).skiplist_writeset;

		WriteSetEntry entry = writeset.get(item);
		if (entry != null) {
			if (entry.operation == ADD) {
				if (type == ADD)
					return false;
				else if (type == CONTAINS)
					return true;
				else // remove
				{
					writeset.remove(item);
					return true;
				}
			} else // remove
			{
				if (type == REMOVE || type == CONTAINS)
					return false;
				else // add
				{
					writeset.remove(item);
					return true;
				}
			}
		}

		ArrayList<ReadSetEntry> readset = ((SetThread) Thread.currentThread()).skiplist_readset;

		OBNode[] preds = (OBNode[]) new OBNode[MAX_LEVEL + 1];
		OBNode[] currs = (OBNode[]) new OBNode[MAX_LEVEL + 1];

		int lFound = find(item, preds, currs);

		if (lFound != -1) {
			OBNode nodeFound = currs[lFound];
			while (!nodeFound.fullyLinked)
				;
			if (nodeFound.marked)
				lFound = -1;
		}

		if (!postValidate(readset))
			throw AbortedException.abortedException;

		if (lFound != -1) // item is in the set
		{

			if (type == CONTAINS) {
				readset.add(new ReadSetEntry(preds, currs, 0, 0));
				return true;
			} else if (type == ADD) {
				readset.add(new ReadSetEntry(preds, currs, 0, 0));
				return false;
			} else // remove
			{
				readset.add(new ReadSetEntry(preds, currs, 2, lFound));
				writeset.put(item, new WriteSetEntry(preds, currs, REMOVE,
						item, lFound));
				return true;
			}
		} else {
			if (type == CONTAINS || type == REMOVE) {
				readset.add(new ReadSetEntry(preds, currs, 1, 0));
				return false;
			} else // add
			{
				int topLevel = randomLevel();
				readset.add(new ReadSetEntry(preds, currs, 2, topLevel));
				writeset.put(item, new WriteSetEntry(preds, currs, ADD, item,
						topLevel));
				return true;
			}
		}
	}

	@Override
	public void commit() throws AbortedException {

		SetThread t = ((SetThread) Thread.currentThread());

		// read-only transactions do nothing
		if (t.skiplist_writeset.isEmpty()) {
			t.skiplist_readset.clear();
			return;
		}

		long threadId = Thread.currentThread().getId();
		Iterator<Entry<Integer, WriteSetEntry>> iterator = t.skiplist_writeset
				.entrySet().iterator();
		WriteSetEntry entry;

		int highestLocked = -1;
		OBNode victim , newNode, pred , curr;
		int victimLock, predLock;

		// Acquire locks
		while (iterator.hasNext()) {
			entry = iterator.next().getValue();
			victim = entry.currs[entry.topLevel];
			if (entry.operation == REMOVE) {
				victimLock = victim.lock.get();
				if ((victimLock & 1) == 1 && victim.lockHolder != threadId)
					throw AbortedException.abortedException;
				if (victim.lockHolder == threadId
						|| victim.lock
								.compareAndSet(victimLock, victimLock + 1))
					victim.lockHolder = threadId;
				else
					throw AbortedException.abortedException;
			}
			for (int level = 0; level <= entry.topLevel; level++) {
				pred = entry.preds[level];
				predLock = pred.lock.get();
				if ((predLock & 1) == 1 && pred.lockHolder != threadId)
				{
					for (int l = 0; l <= highestLocked; l++) {
						if(entry.preds[l].lockHolder == threadId)
						{
							entry.preds[l].lockHolder = -1;
							entry.preds[l].lock.decrementAndGet();
						}
					}
					if (entry.operation == REMOVE && victim.lockHolder == threadId) {
						victim.lockHolder = -1;
						victim.lock.decrementAndGet();
					}
					throw AbortedException.abortedException;
				}
				if (pred.lockHolder == threadId
						|| pred.lock.compareAndSet(predLock, predLock + 1)) {
					highestLocked = level;
					entry.preds[level].lockHolder = threadId;
				} else {
					for (int l = 0; l <= highestLocked; l++) {
						if(entry.preds[l].lockHolder == threadId)
						{
							entry.preds[l].lockHolder = -1;
							entry.preds[l].lock.decrementAndGet();
						}
					}
					if (entry.operation == REMOVE && victim.lockHolder == threadId) {
						victim.lockHolder = -1;
						victim.lock.decrementAndGet();
					}
					throw AbortedException.abortedException;
				}
			}

		}

		// validate read-set
		if (!commitValidate(t.skiplist_readset))
			throw AbortedException.abortedException;
		
		// Publish write-set
		iterator = t.skiplist_writeset.entrySet().iterator();
		while (iterator.hasNext()) {
			entry = iterator.next().getValue();

			// add
			if (entry.operation == ADD) {
				newNode = new OBNode(entry.item, entry.topLevel);
				newNode.lock.set(1);
				newNode.lockHolder = threadId;
				entry.newNode = newNode;
				for (int level = 0; level <= entry.topLevel; level++) {
					// iterate to the new pred and curr
					pred = entry.preds[level];
					curr = entry.preds[level].next[level];
					while (curr.item < entry.item) {
						pred = curr;
						curr = curr.next[level];
					}

					// make the new link on the level
					newNode.next[level] = curr;
					pred.next[level] = newNode;

					// newNode.next[level] = entry.currs[level];
					// entry.preds[level].next[level] = newNode;

				}
				newNode.fullyLinked = true; // successful add linearization
											// point
			} else // remove
			{
				victim = entry.currs[entry.topLevel];
				victim.marked = true;
				for (int level = victim.topLevel; level >= 0; level--) {
					// iterate to the new pred and curr
					pred = entry.preds[level];
					curr = entry.preds[level].next[level];
					while (curr.item < entry.item) {
						pred = curr;
						curr = curr.next[level];
					}
					pred.next[level] = victim.next[level];

					// entry.preds[level].next[level] = victim.next[level];
				}
			}
		}

		// unlock
		iterator = t.skiplist_writeset.entrySet().iterator();
		while (iterator.hasNext()) {
			entry = iterator.next().getValue();
			// victim in this case is either the added or the removed node 
			if(entry.operation == REMOVE)
				victim = entry.currs[entry.topLevel];
			else // add
				victim = entry.newNode;
			if (victim.lockHolder == threadId) {
				victim.lockHolder = -1;
				victim.lock.incrementAndGet();
			}

			for (int level = 0; level <= entry.topLevel; level++) {
				pred = entry.preds[level];
				if (pred.lockHolder == threadId) {
					pred.lockHolder = -1;
					pred.lock.incrementAndGet();
				}
			}
		}

		// clear read- and write- sets
		t.skiplist_readset.clear();
		t.skiplist_writeset.clear();

	}

	@Override
	public void abort() {

		SetThread t = ((SetThread) Thread.currentThread());

		// internally you must not call this method. It's sufficient to throw
		// AbortException
		// This method will be called from calling transaction as a part of its
		// abort subroutine
		// This way: Abort is handled by transaction not by data structure.

		// Unlock
		Iterator<Entry<Integer, WriteSetEntry>> iterator = t.skiplist_writeset
				.entrySet().iterator();
		WriteSetEntry entry;
		OBNode victim, pred;
		while (iterator.hasNext()) {
			entry = iterator.next().getValue();
			victim = entry.currs[entry.topLevel];
			if (entry.operation == REMOVE && victim.lockHolder == t.getId()) {
				victim.lockHolder = -1;
				victim.lock.decrementAndGet();
			}
			for (int level = 0; level <= entry.topLevel; level++) {
				pred = entry.preds[level];
				if (pred.lockHolder == t.getId()) {
					pred.lockHolder = -1;
					pred.lock.decrementAndGet();
				}
			}
		}

		// clear read- and write- sets
		t.skiplist_readset.clear();
		t.skiplist_writeset.clear();
	}

	public boolean nontransactionalAdd(int item) {

		int topLevel = randomLevel();
		OBNode[] preds = (OBNode[]) new OBNode[MAX_LEVEL + 1];
		OBNode[] currs = (OBNode[]) new OBNode[MAX_LEVEL + 1];
		int lFound = find(item, preds, currs);

		if (lFound != -1)
			return false;
		else {
			OBNode newNode = new OBNode(item, topLevel);
			for (int level = 0; level <= topLevel; level++)
				newNode.next[level] = currs[level];
			for (int level = 0; level <= topLevel; level++)
				preds[level].next[level] = newNode;
			newNode.fullyLinked = true; // successful add linearization
										// point
			return true;

		}
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
