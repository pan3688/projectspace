import java.util.concurrent.PriorityBlockingQueue;

public class Synchronized implements IntPQ{

	PriorityBlockingQueue<Integer> heap = new PriorityBlockingQueue<Integer>();
	
	@Override
	public boolean add(int value) throws AbortedException {
		heap.add(value);
		return true;
	}

	@Override
	public int removeMin() throws AbortedException {
		return heap.poll();
	}

	@Override
	public int getMin() throws AbortedException {
		return heap.peek();
	}

	@Override
	public void commit() throws AbortedException {
		
	}

	@Override
	public void abort() {
		
	}

	@Override
	public boolean nontransactionalAdd(int value) {
		heap.add(value);
		return true;
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
