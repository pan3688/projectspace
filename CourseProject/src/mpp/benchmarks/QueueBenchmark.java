package mpp.benchmarks;

import java.util.Random;

import mpp.queue.IntQueue;
import mpp.queue.OptimisticBoostedQueue;
import mpp.queue.TransactionalQueue;

public class QueueBenchmark implements Benchmark {

	IntQueue m_queue;
	int m_range = 1<<16;
	int m_rate = 20;
	int m_ops = 1;
	
	@Override
	public void init(String[] args) {
		boolean error = false;
		int initial = 256;
		
		if(args.length > 0){
			if(args[0].trim().equals("OptimisticBoosted")){
				m_queue = new OptimisticBoostedQueue();
			}else if(args[0].trim().equals("Transactional")){
				m_queue = new TransactionalQueue();
			}else
				error = true;
			
		}else
			error = true;
		
		for(int i=1;i < args.length && !error; i++){
			
			if(args[i].equals("-i")){
				if(++i < args.length)
					initial = Integer.parseInt(args[i]);
				else
					error = true;
			}else if(args[i].equals("-r")){
				if(++i < args.length)
					m_range = Integer.parseInt(args[i]);
				else
					error = true;
			}else if(args[i].equals("-w")){
				if(++i < args.length)
					m_rate = Integer.parseInt(args[i]);
				else
					error = true;
			}else if(args[i].equals("-o")){
				if(++i < args.length)
					m_ops = Integer.parseInt(args[i]);
				else
					error = true;
			}else
				error = true;
		}
		
		if (error) {
			System.out.println("Benchmark arguments: (Synchronized|Transactional|FineGrained|Boosted|OptimisticBoosted) [-o ops-per-transaction] [-i initial-size] [-r range] [-w write-rate]");
			System.exit(1);
		}
		Random random = new Random();
		
		int c = 0;
		for (int i = 0; i < initial ; i++){
			if(m_queue.nonTransactionalEnqueue(random.nextInt(m_range)))
				c++;
		}
		
		System.out.println("Initial size        = " + c);
		System.out.println("Range               = " + m_range);
		System.out.println("Write rate          = " + m_rate + "%");
		System.out.println("Ops per transaction	= " + m_ops);
		System.out.println();
}

	@Override
	public BenchmarkThread createThread(int i, int nb) {
		return new QueueThread(m_queue,m_range,m_rate,m_ops);
	}

	@Override
	public String getStats(BenchmarkThread[] threads) {
		int add = 0;
		int succ_add = 0;
		int remove = 0;
		int succ_remove = 0;
		int element = 0;
		int aborts = 0;
		for (int i = 0; i < threads.length; i++) {
			add += ((QueueThread)threads[i]).m_nb_add;
			succ_add += ((QueueThread)threads[i]).m_nb_succ_add;
			remove += ((QueueThread)threads[i]).m_nb_remove;
			succ_remove += ((QueueThread)threads[i]).m_nb_succ_remove;
			element += ((QueueThread)threads[i]).m_nb_elements;
			aborts += ((QueueThread)threads[i]).m_nb_aborts;
		}
//		System.out.println("final size	= " + m_pq.size());
		return "A=" + add + ", SA=" + succ_add + ", R=" + remove + ", SR=" + succ_remove + ", C=" + element + ", Aborts=" + aborts;
	}

}
