package mpp.benchmarks;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeMap;

import mpp.exception.AbortedException;
import mpp.maps.IntMap;
import mpp.maps.open.OBNode;
import mpp.maps.open.BucketListOpen;
import mpp.maps.open.OptimisticBoostedOpenMap;

public class OpenMapThread extends BenchmarkThread {
	
	final private IntMap m_map;
	final private int m_range;
	final private int m_rate;
	final private int m_ops;
	final private Random m_random;
	
	int m_nb_add;
	int m_nb_remove;
	int m_nb_succ_add;
	int m_nb_succ_remove;
	int m_nb_contains;
	int m_nb_aborts;
	
	
	int[] operationType = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] addItems = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] containsItems = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] removeItems = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	
	int[] m_last = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] oldm_last = new int[OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int add_index = OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION/2, remove_index = 0;
	int old_add_index, old_remove_index;
	boolean m_write;
	
	public TreeMap<Integer, OptimisticBoostedOpenMap.WriteSetEntry> list_writeset = new TreeMap<Integer, OptimisticBoostedOpenMap.WriteSetEntry>(
			new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o2 - o1;
				}
			}
			);
	public ArrayList<OptimisticBoostedOpenMap.ReadSetEntry> list_readset = new ArrayList<OptimisticBoostedOpenMap.ReadSetEntry>();
	
	public BucketListOpen<OBNode>[][] tableLocal;
	public int[][] tableOps;
	public int initialCapacity;
	public boolean resize = false;
	
	public OpenMapThread(IntMap map,int range,int rate,int ops) {
		m_map = map;
		m_range = range;
		m_rate = rate;
		m_ops = ops;
		m_random = new Random();
		m_nb_add = m_nb_remove = m_nb_contains = 0;
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
		
		m_map.begin();
		
		while (flag) {
			flag = false;
			oldm_write = m_write;
			old_add_index = add_index; 
			old_remove_index = remove_index;
			for(int i=0;i< OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION;i++)
				oldm_last[i] = m_last[i];
			try {
				int nb_add = 0, nb_remove = 0, nb_contains = 0, nb_succ_add = 0, nb_succ_remove = 0;
				for (int c = 0; c < m_ops; c++) {
					if (operationType[c] == 1) {
						if (m_write) {
							if(m_map.put(addItems[c], addItems[c] +""))
							{
//								if(initial_adds == MAX_OPERATIONS_PER_TRANSACTION)
									m_write = false;
//								else
//									initial_adds++;
								m_last[add_index] = addItems[c];
								add_index++;
								if(add_index == OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION)
									add_index = 0;
								if(phase == Benchmark.TEST_PHASE){
									System.out.println("Put succeeded....for :" + addItems[c]);
									nb_succ_add++;
								}
							}
							if (phase == Benchmark.TEST_PHASE)
								nb_add++;
						}else{
							if(m_map.remove(addItems[c]) != null)
							{
								if (phase == Benchmark.TEST_PHASE)
									nb_succ_remove++;
							}/*else
								System.out.println("remove is null........for :" + m_last[remove_index]);*/
							remove_index++;
//							System.out.println("REMOVE INDEX:\t\t\t\t" + remove_index);
							if(remove_index == OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION)
								remove_index = 0;
							m_write = true;
							if (phase == Benchmark.TEST_PHASE)
								nb_remove++;
						}
					} else {
						m_map.contains(containsItems[c]);
						if (phase == Benchmark.TEST_PHASE)
							nb_contains++;
					}
				}
				
//				try{
					m_map.commit();
//				}catch(Exception e){
//					e.printStackTrace();
//					throw e;
//				}
				//System.out.println("Thread success:"+ Thread.currentThread().getId());
				m_nb_add += nb_add;
				m_nb_remove += nb_remove;
				m_nb_contains += nb_contains;
				m_nb_succ_add += nb_succ_add;
				m_nb_succ_remove += nb_succ_remove;
				
//				System.out.println("COMMIT: oldaddindex=" + old_add_index + " addindex=" + add_index + " oldremoveindex=" + old_remove_index + " removeindex=" + remove_index);
				
			} catch (AbortedException e) {
				//System.out.println("Thread Aborted:"+ Thread.currentThread().getId());
				flag = true;
				for(int i=0; i<OpenMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION;i++)
					m_last[i] = oldm_last[i];
//				System.out.println("ABORT: oldaddindex=" + old_add_index + " addindex=" + add_index + " oldremoveindex=" + old_remove_index + " removeindex=" + remove_index);
				add_index = old_add_index;
				remove_index = old_remove_index;
				m_write = oldm_write;
				m_nb_aborts++;
				m_map.abort();
			}
		}
	}
	
	@Override
	public String getStats() {
		return "A=" + m_nb_add + ", R=" + m_nb_remove + ", SA=" + m_nb_succ_add + ", SR=" + m_nb_succ_remove + ", C=" + m_nb_contains
				+ ", Aborts=" + m_nb_aborts;
	}	
	
}
