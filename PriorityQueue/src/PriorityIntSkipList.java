import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PriorityIntSkipList implements IntSet {

	static final int MAX_LEVEL = 31;

	static final class Node {
		final Lock lock = new ReentrantLock();
		final int item;
		final Node[] next;
		volatile AtomicBoolean marked = new AtomicBoolean(false);
		volatile boolean fullyLinked = false;
		private int topLevel;

		public Node(int item) { // sentinel node constructor
			this.item = item;
			next = new Node[MAX_LEVEL + 1];
			topLevel = MAX_LEVEL;
		}

		public Node(int item, int height) {
			this.item = item;
			next = new Node[height + 1];
			topLevel = height;
		}

		public void lock() {
			lock.lock();
		}

		public void unlock() {
			lock.unlock();
		}
	}

	final Node head = new Node(Integer.MIN_VALUE);
	final Node tail = new Node(Integer.MAX_VALUE);

	private static final Random seedGenerator = new Random();
	private transient int randomSeed;

	public PriorityIntSkipList() {
		randomSeed = seedGenerator.nextInt() | 0x0100; // ensure nonzero
		for (int i = 0; i < head.next.length; i++) {
			head.next[i] = tail;
		}
	}

	private int randomLevel() {
		int x = randomSeed;
		x ^= x << 13;
		x ^= x >>> 17;
		randomSeed = x ^= x << 5;
		if ((x & 0x8001) != 0) // test highest and lowest bits
			return 0;
		int level = 1;
		while (((x >>>= 1) & 1) != 0)
			++level;
		return level;
	}

	int find(int item, Node[] preds, Node[] succs) {

		int lFound = -1;
		Node pred = head;
		for (int level = MAX_LEVEL; level >= 0; level--) {
			Node curr = pred.next[level];
			while (item > curr.item) {
				pred = curr;
				curr = pred.next[level];
			}
			if (lFound == -1 && item == curr.item) {
				lFound = level;
			}
			preds[level] = pred;
			succs[level] = curr;
		}
		return lFound;
	}

	@Override
	public boolean add(int item) {
		int topLevel = randomLevel();
		Node[] preds = (Node[]) new Node[MAX_LEVEL + 1];
		Node[] succs = (Node[]) new Node[MAX_LEVEL + 1];
		
		int c = 0;
		
		while (true) {
			
			c++;
			if(c>100000)
			{
				c = 0;
				System.out.println("infinite loop 1");
			}
			
			int lFound = find(item, preds, succs);
			if (lFound != -1) {
				Node nodeFound = succs[lFound];
				if (!nodeFound.marked.get()) {
					while (!nodeFound.fullyLinked) {
					}
					return false;
				}
				continue;
			}
			int highestLocked = -1;
			try {
				Node pred, succ;
				boolean valid = true;
				for (int level = 0; valid && (level <= topLevel); level++) {
					pred = preds[level];
					succ = succs[level];
					pred.lock.lock();
					highestLocked = level;
					valid = !pred.marked.get() && !succ.marked.get()
							&& pred.next[level] == succ;
				}
				if (!valid)
					continue;
				Node newNode = new Node(item, topLevel);
				for (int level = 0; level <= topLevel; level++)
					newNode.next[level] = succs[level];
				for (int level = 0; level <= topLevel; level++)
					preds[level].next[level] = newNode;
				newNode.fullyLinked = true; // successful add linearization
											// point
				return true;
			} finally {
				for (int level = 0; level <= highestLocked; level++)
					preds[level].unlock();
			}
		}
	}

	@Override
	public boolean remove(int item) {
		Node victim = null;
		//boolean isMarked = false;
		int topLevel = -1;
		Node[] preds = (Node[]) new Node[MAX_LEVEL + 1];
		Node[] succs = (Node[]) new Node[MAX_LEVEL + 1];
		
		int c = 0;
		
		while (true) {
			
			c++;
			if(c>100000)
			{
				c = 0;
				System.out.println("infinite loop 3");
			}
			
			int lFound = find(item, preds, succs);
			if (lFound != -1)
				victim = succs[lFound];
			else
				// Non reachable
				System.out.println("CANNOT FAIL TO FIND MIN WHICH IS ALREADY MARKED BY ME");
			
			// NO NEED HERE TO TEST 'MARKED' BECAUSE IT'S ALREADY 
			// LOGICALLY DELETED BY findAndMarkMin() 
			topLevel = victim.topLevel;
//			if (isMarked
//					| (lFound != -1 && (victim.fullyLinked
//							&& victim.topLevel == lFound && !victim.marked))) {
//				if (!isMarked) {
//					topLevel = victim.topLevel;
					victim.lock.lock();
//					if (victim.marked) {
//						victim.lock.unlock();
//						return false;
//					}
//					victim.marked = true;
//					isMarked = true;
//				}
				int highestLocked = -1;
				try {
					Node pred;
					boolean valid = true;
					for (int level = 0; valid && (level <= topLevel); level++) {
						pred = preds[level];
						pred.lock.lock();
						highestLocked = level;
						valid = !pred.marked.get() && pred.next[level] == victim;
					}
					if (!valid)
						continue;
					for (int level = topLevel; level >= 0; level--) {
						preds[level].next[level] = victim.next[level];
					}
					return true;
				} finally {
					victim.lock.unlock();
					for (int i = 0; i <= highestLocked; i++) {
						preds[i].unlock();
					}
				}
			// Cannot return false because it's already logically deleted
			// removeMin will only return false if findAndMarkMin() returns null
//			} else
//				return false;
		}
	}

	@Override
	public boolean contains(int item) {
		Node[] preds = (Node[]) new Node[MAX_LEVEL + 1];
		Node[] succs = (Node[]) new Node[MAX_LEVEL + 1];
		int lFound = find(item, preds, succs);
		return (lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked.get());
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

	public Node findAndMarkMin() {
		Node curr = null;
		curr = head.next[0];
		int c = 0;
		while (curr != tail) {
			
			// wait until node is fully linked
			while(!curr.fullyLinked);
			
			if (!curr.marked.get()) {
				if (curr.marked.compareAndSet(false, true)) {
					return curr;
				} else {
					curr = curr.next[0];
				}
			}
			else
				curr = curr.next[0];
			
			c++;
			if(c==100000)
			{
				c=0;
				System.out.println("infinite loop");
			}
		}
		System.out.println("removeMin: no unmarked nodes");
		return null; // no unmarked nodes
	}
	
	public Node findMin() {
		Node curr = null;
		curr = head.next[0];
		int c = 0;
		while (curr != tail) {
			
			// wait until node is fully linked
			while(!curr.fullyLinked);
			
			if (!curr.marked.get())
				return curr;
			else
				curr = curr.next[0];
			
			c++;
			if(c>100000)
			{
				c=0;
				System.out.println("infinite loop 2");
			}
			
		}
		System.out.println("getMin: no unmarked nodes");
		return null; // no unmarked nodes
	}

}
