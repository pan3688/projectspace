
public class Transactional extends java.util.PriorityQueue<Integer> implements IntPQ{
	
	private static final long serialVersionUID = 1L;

	@Override
//	@Atomic
	public boolean add(int value) throws AbortedException {
		return super.add(value);
	}

	@Override
//	@Atomic
	public int removeMin() throws AbortedException {
		return super.poll();
	}

	@Override
//	@Atomic
	public int getMin() throws AbortedException {
		return super.peek();
	}

	@Override
	public void commit() throws AbortedException {
		
	}

	@Override
	public void abort() {
		
	}

	@Override
	public boolean nontransactionalAdd(int value) {
		return super.add(value);
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}
}
