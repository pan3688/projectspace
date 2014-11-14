

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class SetThread extends BenchmarkThread {
	final private IntSet m_set;
	final private int m_range;
	final private int m_rate;
	final private int m_ops;
	int m_nb_add;
	int m_nb_remove;
	int m_nb_succ_add;
	int m_nb_succ_remove;
	int m_nb_contains;
	int m_nb_aborts;
	
	
	int[] operationType = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] addItems = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] containsItems = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] removeItems = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	
	int[] m_last = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] oldm_last = new int[SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int add_index = SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION/2, remove_index = 0;
	int old_add_index, old_remove_index;
	boolean m_write;
	
	final private Random m_random;
	
	int localVersion = 0;

	// //////////////////////////////////////////////////////////////
	// for boosting
	Stack<BoostedList.LogEntry> list_log = new Stack<BoostedList.LogEntry>();
	Stack<BoostedSkipList.LogEntry> skiplist_log = new Stack<BoostedSkipList.LogEntry>();
	HashMap<Integer, Lock> lockSet = new HashMap<Integer, Lock>();
	// for boosting
	// //////////////////////////////////////////////////////////////

	// //////////////////////////////////////////////////////////////
	// for optimistic boosting
	
	TreeMap<Integer, OptimisticBoostedList.WriteSetEntry> list_writeset = new TreeMap<Integer, OptimisticBoostedList.WriteSetEntry>(
			new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o2 - o1;
				}
			}
			);
	ArrayList<OptimisticBoostedList.ReadSetEntry> list_readset = new ArrayList<OptimisticBoostedList.ReadSetEntry>();
	
	TreeMap<Integer, OptimisticBoostedSkipList.WriteSetEntry> skiplist_writeset = new TreeMap<Integer, OptimisticBoostedSkipList.WriteSetEntry>(
			new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o2 - o1;
				}
			}
			);
	ArrayList<OptimisticBoostedSkipList.ReadSetEntry> skiplist_readset = new ArrayList<OptimisticBoostedSkipList.ReadSetEntry>();
	// for optimistic boosting
	// //////////////////////////////////////////////////////////////

	public SetThread(IntSet set, int range, int rate, int ops) {
		m_set = set;
		m_range = range;
		m_ops = ops;
		m_nb_add = m_nb_remove = m_nb_contains = 0;
		m_rate = rate;
		m_write = true;
		m_random = new Random();
	}

	protected void step(int phase) {
		
		for(int c = 0; c < m_ops; c++)
		{
			int i = m_random.nextInt(100);
			if (i < m_rate)
				operationType[c] = 1; // add or remove
			else
				operationType[c] = 0; // contains
			
			//removeItems[c] = addItems[c];
			addItems[c] = m_random.nextInt(m_range);
			containsItems[c] = m_random.nextInt(m_range);
		}
		
		boolean flag = true, oldm_write = m_write;
		
		while (flag) {
			flag = false;
			oldm_write = m_write;
			old_add_index = add_index; 
			old_remove_index = remove_index;
			for(int i=0;i<SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION;i++)
				oldm_last[i] = m_last[i];
			try {
				int nb_add = 0, nb_remove = 0, nb_contains = 0, nb_succ_add = 0, nb_succ_remove = 0;
				for (int c = 0; c < m_ops; c++) {
					if (operationType[c] == 1) {
						if (m_write) {
							if(m_set.add(addItems[c]))
							{
//								if(initial_adds == MAX_OPERATIONS_PER_TRANSACTION)
									m_write = false;
//								else
//									initial_adds++;
								m_last[add_index] = addItems[c];
								add_index++;
								if(add_index == SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION)
									add_index = 0;
								if(phase == Benchmark.TEST_PHASE)
									nb_succ_add++;
							}
							if (phase == Benchmark.TEST_PHASE)
								nb_add++;
						} else {
							if(m_set.remove(m_last[remove_index]))
							{
								if (phase == Benchmark.TEST_PHASE)
									nb_succ_remove++;
							}
							remove_index++;
							if(remove_index == SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION)
								remove_index = 0;
							m_write = true;
							if (phase == Benchmark.TEST_PHASE)
								nb_remove++;
						}
					} else {
						m_set.contains(containsItems[c]);
						if (phase == Benchmark.TEST_PHASE)
							nb_contains++;
					}
				}
				m_set.commit();
				m_nb_add += nb_add;
				m_nb_remove += nb_remove;
				m_nb_contains += nb_contains;
				m_nb_succ_add += nb_succ_add;
				m_nb_succ_remove += nb_succ_remove;
				
//				System.out.println("COMMIT: oldaddindex=" + old_add_index + " addindex=" + add_index + " oldremoveindex=" + old_remove_index + " removeindex=" + remove_index);
				
			} catch (AbortedException e) {
				flag = true;
				for(int i=0; i<SetBenchmark.MAX_OPERATIONS_PER_TRANSACTION;i++)
					m_last[i] = oldm_last[i];
//				System.out.println("ABORT: oldaddindex=" + old_add_index + " addindex=" + add_index + " oldremoveindex=" + old_remove_index + " removeindex=" + remove_index);
				add_index = old_add_index;
				remove_index = old_remove_index;
				m_write = oldm_write;
				m_nb_aborts++;
				m_set.abort();
			}
		}
	}

	public String getStats() {
		return "A=" + m_nb_add + ", R=" + m_nb_remove + ", SA=" + m_nb_succ_add + ", SR=" + m_nb_succ_remove + ", C=" + m_nb_contains
				+ ", Aborts=" + m_nb_aborts;
	}
}
