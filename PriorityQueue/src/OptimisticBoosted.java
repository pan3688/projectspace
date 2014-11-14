import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OptimisticBoosted implements IntPQ {

	private final Lock lock = new ReentrantLock();
	PriorityQueue<Integer> heap = new PriorityQueue<Integer>();

	@Override
	public boolean add(int value) throws AbortedException {
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			if (t.iswriter)
				heap.add(value);
			else
				t.localAdds.add(value);
			return true;
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}
	}

	@Override
	public int removeMin() throws AbortedException {
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			if (!t.iswriter) {
				lock.lock();
				t.iswriter = true;

				// publish previous adds
				int size = t.localAdds.size();
				for (int i = 0; i < size; i++)
					heap.add(t.localAdds.get(i));
			}

			return heap.poll();
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}

	}

	@Override
	public int getMin() throws AbortedException {
		try {
			PQThread t = ((PQThread) Thread.currentThread());
			if (!t.iswriter) {
				t.iswriter = true;
				lock.lock();

				// publish previous adds
				int size = t.localAdds.size();
				for (int i = 0; i < size; i++)
					heap.add(t.localAdds.get(i));
			}

			return heap.peek();
		} catch (Exception e) {
			throw AbortedException.abortedException;
		}

	}

	@Override
	public void commit() throws AbortedException {
		PQThread t = ((PQThread) Thread.currentThread());
		if (t.iswriter) {
			t.iswriter = false;
			lock.unlock();
		} else if (!t.localAdds.isEmpty()) {
			lock.lock();

			// publish adds
			int size = t.localAdds.size();
			for (int i = 0; i < size; i++)
				heap.add(t.localAdds.get(i));

			lock.unlock();
		}
		t.localAdds.clear();
	}

	@Override
	public void abort() {
		PQThread t = ((PQThread) Thread.currentThread());
		t.localAdds.clear();
		if(t.iswriter)
		{
			t.iswriter = false;
			lock.unlock();
		}
	}

	@Override
	public boolean nontransactionalAdd(int item) {
		heap.add(item);
		return true;
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}
}
