

public interface IntPQ {
	public void begin();
	
	public boolean add(int value) throws AbortedException;

	public int removeMin() throws AbortedException;

	public int getMin() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nontransactionalAdd(int item);
}
