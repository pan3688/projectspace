


public interface IntSet {
	
	public void begin();
	
	public boolean add(int value) throws AbortedException;

	public boolean remove(int value) throws AbortedException;

	public boolean contains(int value) throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nontransactionalAdd(int item);
}
