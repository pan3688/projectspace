


public interface IntHolderSet {
	
	public void begin();
	
	public boolean add(Holder value) throws AbortedException;

	public boolean remove(Holder value) throws AbortedException;

	public boolean contains(Holder value) throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nontransactionalAdd(Holder item);
}
