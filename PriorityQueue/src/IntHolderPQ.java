

public interface IntHolderPQ {
	public boolean add(Holder value) throws AbortedException;

	public Holder removeMin() throws AbortedException;

	public Holder getMin() throws AbortedException;
	
	public void commit() throws AbortedException;
	
	public void abort();
	
	public boolean nontransactionalAdd(Holder item);
}
