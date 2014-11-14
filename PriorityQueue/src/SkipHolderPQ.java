public class SkipHolderPQ implements IntHolderPQ {

	PriorityHolderSkipList skipList;

	public SkipHolderPQ() {
		skipList = new PriorityHolderSkipList();
	}

	@Override
	public boolean add(Holder item) throws AbortedException {
		return skipList.add(item);
	}

	@Override
	public Holder removeMin() throws AbortedException {
		PriorityHolderSkipList.Node node = skipList.findAndMarkMin();
		if (node != null) {
			skipList.remove(node.item);
			return node.item;
		} else {
			// List is empty
			return null;
		}
	}

	@Override
	public Holder getMin() throws AbortedException {
		PriorityHolderSkipList.Node node = skipList.findMin();
		if(node != null)
			return node.item;
		else
			return null;
	}

	@Override
	public void commit() throws AbortedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public boolean nontransactionalAdd(Holder item) {
		try {
			return add(item);
		} catch (AbortedException e) {
			System.err.println("cannot happen");
			return false;
		}
	}

}
