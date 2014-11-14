import java.util.Stack;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Boosted implements IntPQ {

	class Holder implements Comparable<Holder> {
		int item;
		boolean deleted;

		public Holder(int item) {
			this.item = item;
			this.deleted = false;
		}

		@Override
		public int compareTo(Holder h) {
			return h.item - this.item;
		}
	}

	public abstract class LogEntry {
		abstract void undo();
	}

	PriorityBlockingQueue<Holder> heap = new PriorityBlockingQueue<Holder>();
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock read = lock.readLock();
	private final Lock write = lock.writeLock();

	@Override
	public boolean add(int value) throws AbortedException {

		// Acquire read lock if no lock is previously acquired
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			if (!t.iswriter && !t.isReader) {
				read.lock();
				t.isReader = true;
			}

			final Holder holder = new Holder(value);
			heap.add(holder);

			LogEntry e = new LogEntry() {
				public void undo() {
					holder.deleted = true;
				}
			};
			t.pesBoostedLog.push(e);
			return true;
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}
	}

	@Override
	public int removeMin() throws AbortedException {
		try {
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
				holder = heap.poll();
			} while (holder.deleted);
			if (holder != null) {
				final int result = holder.item;
				LogEntry e = new LogEntry() {
					public void undo() {
						heap.add(new Holder(result));
					}
				};
				t.pesBoostedLog.push(e);
			}
			return holder.item;
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}
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
			
			while(heap.peek().deleted)
				heap.poll();
			return heap.peek().item;
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
		Stack<LogEntry> log = t.pesBoostedLog;
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
		heap.add(new Holder(item));
		return true;
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
