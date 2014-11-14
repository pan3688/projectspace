
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;

public class PQThread extends BenchmarkThread {
	final private IntPQ m_pq;
	final private int m_range;
	final private int m_rate;
	final private int m_ops;
	int m_nb_add;
	int m_nb_succ_add;
	int m_nb_remove;
	int m_nb_succ_remove;
	int m_nb_contains;
	int m_nb_aborts;
	boolean m_write;
	int m_last;
	
	final int MAX_OPERATIONS_PER_TRANSACTION = 20;	
	
	int[] executionOperations = new int[20];
	int[] executionItems = new int[20];
	
	final private Random m_random;
	
	// //////////////////////////////////////////////////////////////
	// for boosting
	
	// local undo log for pessimistic boosting
	Stack<Boosted.LogEntry> pesBoostedLog = new Stack<Boosted.LogEntry>();
	Stack<BoostedSkipPQ.LogEntry> pesBoostedSkipLog = new Stack<BoostedSkipPQ.LogEntry>();
	
	// local redo log for optimistic boosting
	ArrayList<Integer> localAdds = new ArrayList<Integer>();
	
	boolean isReader = false;
	boolean iswriter = false;
	
	// for boosting
	// //////////////////////////////////////////////////////////////
	
	// //////////////////////////////////////////////////////////////
	// for optimistic boosted skipPQ
	
	TreeMap<Integer, OptimisticBoostedPrioritySkipList.WriteSetEntry> skiplist_writeset = new TreeMap<Integer, OptimisticBoostedPrioritySkipList.WriteSetEntry>(
			new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o2 - o1;
				}
			}
			);
	
	PriorityQueue<Integer> OBLocalAdds = new PriorityQueue<Integer>(
			MAX_OPERATIONS_PER_TRANSACTION	, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o1 - o2;
				}
			}
			);
	ArrayList<OptimisticBoostedPrioritySkipList.ReadSetEntry> skiplist_readset = new ArrayList<OptimisticBoostedPrioritySkipList.ReadSetEntry>();
	OptimisticBoostedPrioritySkipList.OBNode firstMin = null;
	OptimisticBoostedPrioritySkipList.OBNode lastRemovedMin = null;
	// for optimistic boosting
	// //////////////////////////////////////////////////////////////


	public PQThread(IntPQ pq, int range, int rate, int ops) {
		m_pq = pq;
		m_range = range;
		m_ops = ops;
		m_nb_add = m_nb_succ_add = m_nb_remove = m_nb_succ_remove = m_nb_contains = 0;
		m_rate = rate;
		m_write = true;
		m_random = new Random();
	}

	protected void step(int phase) {
		
		for(int c = 0; c < m_ops; c++)
			executionItems[c] = m_random.nextInt(m_range);
		boolean flag = true, oldm_write = m_write;
		int oldm_last = m_last;
		boolean result = false;
		
		int i = m_random.nextInt(100);
		if (i < m_rate)
			executionOperations[0] = 1; // add or remove
		else
			executionOperations[0] = 0; // contains
		
		while (flag) {
			flag = false;
			oldm_write = m_write;
			oldm_last = m_last;
			m_pq.begin();
			try {
				int nb_add = 0, nb_succ_add = 0, nb_remove = 0, nb_succ_remove = 0, nb_contains = 0;
				for (int c = 0; c < m_ops; c++) {
					if (executionOperations[0] == 1) {
						if (m_write) {
							m_last = executionItems[c];
							result = m_pq.add(m_last);
							if(result)
								m_write = false;
							if (phase == Benchmark.TEST_PHASE)
							{
								nb_add++;
								if(result == true)
									nb_succ_add++;
							}
						} else {
							if(m_pq.removeMin() != -1)
							{
								result = true;
								m_write = true;
							}
							if (phase == Benchmark.TEST_PHASE)
							{
								nb_remove++;
								if(result == true)
									nb_succ_remove++;
							}
						}
					} else {
						m_pq.getMin();
						if (phase == Benchmark.TEST_PHASE)
							nb_contains++;
					}
				}
				m_pq.commit();
				m_nb_add += nb_add;
				m_nb_succ_add += nb_succ_add;
				m_nb_remove += nb_remove;
				m_nb_succ_remove += nb_succ_remove;
				m_nb_contains += nb_contains;
//				m_write = !m_write;
			} catch (AbortedException e) {
				flag = true;
				m_last = oldm_last;
				m_write = oldm_write;
				m_nb_aborts++;
				m_pq.abort();
			}
		}
	}
	
	protected void step_mixed(int phase) {
		
		for(int c = 0; c < m_ops; c++)
		{
			int i = m_random.nextInt(100);
			if (i < m_rate)
				executionOperations[c] = 1; // add or remove
			else
				executionOperations[c] = 0; // contains
			executionItems[c] = m_random.nextInt(m_range);
		}
		boolean flag = true, oldm_write = m_write;
		int oldm_last = m_last;
		
		while (flag) {
			flag = false;
			oldm_write = m_write;
			oldm_last = m_last;
			try {
				int nb_add = 0, nb_succ_add = 0, nb_remove = 0, nb_succ_remove = 0, nb_contains = 0;
				for (int c = 0; c < m_ops; c++) {
					if (executionOperations[c] == 1) {
						if (m_write) {
							m_last = executionItems[c];
							if(m_pq.add(m_last))
								m_write = false;
							if (phase == Benchmark.TEST_PHASE)
							{
								nb_add++;
								if(m_write == false)
									nb_succ_add++;
							}
						} else {
							if(m_pq.removeMin() != -1)
								m_write = true;
							if (phase == Benchmark.TEST_PHASE)
							{
								nb_remove++;
								if(m_write == true)
									nb_succ_remove++;
							}
						}
					} else {
						m_pq.getMin();
						if (phase == Benchmark.TEST_PHASE)
							nb_contains++;
					}
				}
				m_pq.commit();
				m_nb_add += nb_add;
				m_nb_succ_add += nb_succ_add;
				m_nb_remove += nb_remove;
				m_nb_succ_remove += nb_succ_remove;
				m_nb_contains += nb_contains;
			} catch (AbortedException e) {
				flag = true;
				m_last = oldm_last;
				m_write = oldm_write;
				m_nb_aborts++;
				m_pq.abort();
			}
		}
	}

	public String getStats() {
		return "A=" + m_nb_add + ", SA=" + m_nb_succ_add + ", R=" + m_nb_remove + ", SR=" + m_nb_succ_remove + ", C=" + m_nb_contains
				+ ", Aborts=" + m_nb_aborts;
	}
}
