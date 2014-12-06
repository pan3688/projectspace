package mpp.benchmarks;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeMap;

import mpp.maps.IntMap;
import mpp.maps.OptimisticBoostedClosedMap;
import mpp.maps.OptimisticBoostedOpenMap;

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
	
	
	int[] operationType = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] addItems = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] containsItems = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] removeItems = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	
	int[] m_last = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int[] oldm_last = new int[ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION];
	int add_index = ClosedMapBenchmark.MAX_OPERATIONS_PER_TRANSACTION/2, remove_index = 0;
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
}
