
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LazyList implements IntSet {

	private class Node {
		int item;
		boolean marked;
		Node next;
		Lock lock = new ReentrantLock();

		Node(int item) {
			this.item = item;
			marked = false;
		}
	}

	Node head = new Node(-1);
	Node tail = new Node(1000000);

	public LazyList() {
		head.next = tail;
		tail.next = tail;
	}

	private boolean validate(Node pred, Node curr) {
		return !pred.marked && !curr.marked && pred.next == curr;
	}

	@Override
	public boolean add(int item) {
		while (true) {
			Node pred = head;
			Node curr = head.next;
			while (curr.item < item) {
				pred = curr;
				curr = curr.next;
			}
			pred.lock.lock();
			try {
//				curr.lock.lock();
//				try {
					if (validate(pred, curr)) {
						if (curr.item == item) {
							return false;
						} else {
							Node node = new Node(item);
							node.next = curr;
							pred.next = node;
							return true;
						}
					}
//				} finally {
//					curr.lock.unlock();
//				}
			} finally {
				pred.lock.unlock();
			}
		}
	}

	@Override
	public boolean remove(int item) {
		while (true) {
			Node pred = head;
			Node curr = head.next;
			while (curr.item < item) {
				pred = curr;
				curr = curr.next;
			}
			pred.lock.lock();
			try {
				curr.lock.lock();
				try {
					if (validate(pred, curr)) {
						if (curr.item != item) {
							return false;
						} else {
							curr.marked = true;
							pred.next = curr.next;
							return true;
						}
					}
				} finally {
					curr.lock.unlock();
				}
			} finally {
				pred.lock.unlock();
			}
		}
	}

	@Override
	public boolean contains(int item) {
		Node curr = head;
		while (curr.item < item)
			curr = curr.next;
		return curr.item == item && !curr.marked;
	}
	
	@Override
	public void begin() {
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}
	
	public boolean nontransactionalAdd(int item)
	{
		return add(item);
	}

}
