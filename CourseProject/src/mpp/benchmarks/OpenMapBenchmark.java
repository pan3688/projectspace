package mpp.benchmarks;

import java.util.Random;

import mpp.maps.IntMap;
import mpp.maps.open.OptimisticBoostedOpenMap;

public class OpenMapBenchmark implements Benchmark {

	IntMap m_map;
	int m_range = 1 << 16;
	int m_rate = 20;
	int m_ops = 1;
	int nb_threads = 1;
	int capacity = 4;
	final static int MAX_OPERATIONS_PER_TRANSACTION = 10;
	
	@Override
	public void init(String[] args) {
		// TODO Auto-generated method stub
		boolean error = false;
		int initial = 256;
		
		if(args.length > 0){
			if(args[0].trim().equals("OptimisticBoosted")){
				m_map = new OptimisticBoostedOpenMap(capacity);
			}else
				error = true;
		}else
			error = true;
		
		for (int i = 1; i < args.length && !error; i++) {
			if (args[i].equals("-i")) {
				if (++i < args.length)
					initial = Integer.parseInt(args[i]);
				else
					error = true;
			} else if (args[i].equals("-r")) {
				if (++i < args.length)
					m_range = Integer.parseInt(args[i]);
				else
					error = true;
			} else if (args[i].equals("-w")) {
				if (++i < args.length)
					m_rate = Integer.parseInt(args[i]);
				else
					error = true;
			} else if (args[i].equals("-o")) {
				if (++i < args.length)
					m_ops = Integer.parseInt(args[i]);
				else
					error = true;
			} else
				error = true;
		}
		if (error) {
			System.out
					.println("Benchmark arguments: (LockFree|Lazy|Boosted|OptimisticBoosted) [-o ops-per-transaction] [-i initial-size] [-r range] [-w write-rate]");
			System.exit(1);
		}
		Random random = new Random();
		int c = 0;
		
		int deferred_adds = nb_threads * MAX_OPERATIONS_PER_TRANSACTION / 2;
		for (int i = 0; i < initial - deferred_adds; i++) {
			if (m_map.nonTransactionalPut(random.nextInt(m_range), new String()))
				c++;
		}
		System.out.println("Initial size        = "
				+ (c + (nb_threads * MAX_OPERATIONS_PER_TRANSACTION / 2)));
		System.out.println("Range               = " + m_range);
		System.out.println("Write rate          = " + m_rate + "%");
		System.out.println("Ops per transaction	= " + m_ops);
		System.out.println();
	}

	@Override
	public BenchmarkThread createThread(int i, int nb) {
		return new OpenMapThread(m_map, m_range, m_rate, m_ops);
	}

	@Override
	public String getStats(BenchmarkThread[] threads) {
		int add = 0;
		int remove = 0;
		int succ_add = 0;
		int succ_remove = 0;
		int contains = 0;
		int aborts = 0;
		for (int i = 0; i < threads.length; i++) {
			add += ((OpenMapThread) threads[i]).m_nb_add;
			remove += ((OpenMapThread) threads[i]).m_nb_remove;
			succ_add += ((OpenMapThread) threads[i]).m_nb_succ_add;
			succ_remove += ((OpenMapThread) threads[i]).m_nb_succ_remove;
			contains += ((OpenMapThread) threads[i]).m_nb_contains;
			aborts += ((OpenMapThread) threads[i]).m_nb_aborts;
		}
		return "A=" + add + ", R=" + remove + ", SA=" + succ_add + ", SR="
				+ succ_remove + ", C=" + contains + ", Aborts=" + aborts;
	}

}
