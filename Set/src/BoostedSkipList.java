

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BoostedSkipList implements IntSet {

	LazySkipList list = new LazySkipList();
	LockKey lock = new LockKey();
	final int LOCK_TIMEOUT = 50;  // in microseconds (large enough to reduce aborts and small enough no to hang)
	
	static volatile boolean result = false;
	
	public abstract class LogEntry
	{
		abstract void undo();
	}
	public class LockKey {
		ConcurrentHashMap<Integer, Lock> map;

		public LockKey() {
			map = new ConcurrentHashMap<Integer, Lock>();
		}

		public void lock(int key) throws AbortedException {
			Lock lock = map.get(key);
			if (lock == null) {
				Lock newLock = new ReentrantLock();
				Lock oldLock = map.putIfAbsent(key, newLock);
				lock = (oldLock == null) ? newLock : oldLock;
			}
			HashMap<Integer, Lock> lockSet = ((SetThread) Thread.currentThread()).lockSet;
			if (lockSet.put(key, lock) == null) {
				try {
					if (!lock.tryLock(LOCK_TIMEOUT, TimeUnit.MICROSECONDS)) {
						lockSet.remove(key);
						throw new AbortedException();
					}
				} catch (InterruptedException e) {
					lockSet.remove(key);
					throw new AbortedException();
				}
//				lock.lock();
			}
		}

		public void unlock(int key) {
			map.get(key).unlock();
			((SetThread) Thread.currentThread()).lockSet.remove(key);
		}
	}
	
	private void unlockAll() {
		HashMap<Integer, Lock> lockSet = ((SetThread) Thread.currentThread()).lockSet;
		Iterator<Entry<Integer, Lock>> iterator = lockSet.entrySet().iterator();
		while(iterator.hasNext())
			iterator.next().getValue().unlock();
		lockSet.clear();
	}

	@Override
	public boolean add(final int value) throws AbortedException {
		lock.lock(value);
		boolean result = list.add(value);
		if (result) {
			LogEntry e = new LogEntry() {
				public void undo() {
					list.remove(value);
				}
			};
			((SetThread) Thread.currentThread()).skiplist_log.push(e);
		}
		return result;
	}

	@Override
	public boolean remove(final int value) throws AbortedException {
		lock.lock(value);
		boolean result = list.remove(value);
		if (result) {
			LogEntry e = new LogEntry() {
				public void undo() {
					list.add(value);
				}
			};
			((SetThread) Thread.currentThread()).skiplist_log.push(e);
		}
		return result;
	}

	@Override
	public boolean contains(final int value) throws AbortedException {
		lock.lock(value);
		result = list.contains(value); 
		return result;
	}
	
	@Override
	public void begin() {
		
	}

	@Override
	public void commit() {
		((SetThread) Thread.currentThread()).list_log.clear();
		unlockAll();
	}

	@Override
	public void abort() {
		Stack<LogEntry> log = ((SetThread) Thread.currentThread()).skiplist_log;
		while(!log.empty())
			log.pop().undo();
		
		unlockAll();
	}
	
	public boolean nontransactionalAdd(int value)
	{
		return list.add(value);
	}
}
