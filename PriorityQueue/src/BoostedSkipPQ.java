import java.util.Stack;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BoostedSkipPQ implements IntPQ {

	public abstract class LogEntry {
		abstract void undo();
	}

	SkipHolderPQ pq = new SkipHolderPQ();
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock read = lock.readLock();
	private final Lock write = lock.writeLock();

	@Override
	public boolean add(int value) throws AbortedException {

		boolean result;
		
		// Acquire read lock if no lock is previously acquired
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			if (!t.iswriter && !t.isReader) {
				read.lock();
				t.isReader = true;
			}

			final Holder holder = new Holder(value);
			result = pq.add(holder);

			LogEntry e = new LogEntry() {
				public void undo() {
					holder.deleted = true;
				}
			};
			t.pesBoostedSkipLog.push(e);
			return result;
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}
	}

	@Override
	public int removeMin() throws AbortedException {
			PQThread t = ((PQThread) Thread.currentThread());
			Holder holder;

			// release read-lock if acquired
			// Acquire write lock if not previously acquired
			if (!t.iswriter) {
				if (t.isReader) {
					read.unlock();
					t.isReader = false;
				}
				write.lock();
				t.iswriter = true;
			}

			do {
				holder = pq.removeMin();
			} while (holder != null && holder.deleted);
			if (holder != null) {
				final int result = holder.value;
				LogEntry e = new LogEntry() {
					public void undo() {
						try {
							pq.add(new Holder(result));
						} catch (AbortedException e) {
							// Not supposed to happen
							e.printStackTrace();
						}
					}
				};
				t.pesBoostedSkipLog.push(e);
				return result;
			}
			return -1;
	}

	@Override
	public int getMin() throws AbortedException {
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			// release read-lock if acquired
			// Acquire write lock if not previously acquired
			if (!t.iswriter) {
				if (t.isReader) {
					read.unlock();
					t.isReader = false;
				}
				write.lock();
				t.iswriter = true;
			}
			
			
			Holder holder = pq.getMin();
			
			while(holder != null && holder.deleted)
			{
				pq.removeMin();
				holder = pq.getMin();
			}
			if(holder != null)
				return holder.value;
			else
				return -1;
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}
	}

	@Override
	public void commit() throws AbortedException {
		PQThread t = ((PQThread) Thread.currentThread());
		t.pesBoostedLog.clear();
		unlockAll(t);
	}

	@Override
	// Probably will not be called in our benchmark because we are using unbounded
	// priority queue, and we are not putting timeouts for locks
	// The only case of calling is when exception occurs during locking
	public void abort() {
		PQThread t = ((PQThread) Thread.currentThread());
		Stack<LogEntry> log = t.pesBoostedSkipLog;
		while (!log.empty())
			log.pop().undo();
		t.pesBoostedLog.clear();
		unlockAll(t);
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
		try {
			return pq.add(new Holder(item));
		} catch (AbortedException e) {
			// Not supposed to happen
			return false;
		}
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
