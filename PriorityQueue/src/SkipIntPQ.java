public class SkipIntPQ implements IntPQ {

	PriorityIntSkipList skipList;

	public SkipIntPQ() {
		skipList = new PriorityIntSkipList();
	}

	@Override
	public boolean add(int item) throws AbortedException {
		return skipList.add(item);
	}

	@Override
	public int removeMin() throws AbortedException {
		PriorityIntSkipList.Node node = skipList.findAndMarkMin();
		if (node != null) {
			skipList.remove(node.item);
			return node.item;
		} else {
			// List is empty
			return -1;
		}
	}

	@Override
	public int getMin() throws AbortedException {
		PriorityIntSkipList.Node node = skipList.findMin();
		if(node != null)
			return node.item;
		else
			return -1;
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
	public boolean nontransactionalAdd(int item){
		try {
			return add(item);
		} catch (AbortedException e) {
			System.err.println("cannot happen");
			return false;
		}
	}

	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

}
