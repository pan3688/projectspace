import java.util.concurrent.atomic.AtomicMarkableReference;

public class LockFreeList implements IntSet {

	private class Node {
		int item;
		AtomicMarkableReference<Node> next;

		Node(int item) {
			this.item = item;
		}
	}

	Node head = new Node(-1);
	Node tail = new Node(1000000);

	public LockFreeList() {
		head.next = new AtomicMarkableReference<Node>(tail, false);
		tail.next = new AtomicMarkableReference<Node>(tail, false);
	}

	class Window {
		public Node pred, curr;

		Window(Node myPred, Node myCurr) {
			pred = myPred;
			curr = myCurr;
		}
	}

	public Window find(Node head, int item) {
		Node pred = null, curr = null, succ = null;
		boolean[] marked = { false };
		boolean snip;
		retry: while (true) {
			pred = head;
			curr = pred.next.getReference();
			while (true) {
				succ = curr.next.get(marked);
				while (marked[0]) {
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if (!snip)
						continue retry;
					curr = succ;
					succ = curr.next.get(marked);
				}
				if (curr.item >= item)
					return new Window(pred, curr);
				pred = curr;
				curr = succ;
			}
		}
	}

	@Override
	public boolean add(int item) {
		while (true) {
			Window window = find(head, item);
			Node pred = window.pred, curr = window.curr;
			if (curr.item == item) {
				return false;
			} else {
				Node node = new Node(item);
				node.next = new AtomicMarkableReference<Node>(curr, false);
				if (pred.next.compareAndSet(curr, node, false, false)) {
					return true;
				}
			}
		}
	}

	@Override
	public boolean remove(int item) {
		boolean snip;
		while (true) {
			Window window = find(head, item);
			Node pred = window.pred, curr = window.curr;
			if (curr.item != item) {
				return false;
			} else {
				Node succ = curr.next.getReference();
				snip = curr.next.attemptMark(succ, true);
				if (!snip)
					continue;
				pred.next.compareAndSet(curr, succ, false, false);
				return true;
			}
		}
	}

	@Override
	public boolean contains(int item) {
		boolean[] marked = { false };
		Node curr = head;
		while (curr.item < item) {
			curr = curr.next.getReference();
		}
		return (curr.item == item && !marked[0]);
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

	public boolean nontransactionalAdd(int item) {
		return add(item);
	}
}
