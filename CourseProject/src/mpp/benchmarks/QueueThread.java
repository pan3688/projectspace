package mpp.benchmarks;

import java.util.ArrayList;
import java.util.Random;

import mpp.exception.AbortedException;
import mpp.queue.IntQueue;
import mpp.queue.OptimisticBoostedQueue.OBNode;

public class QueueThread extends BenchmarkThread {

	final private IntQueue m_queue;
	final private int m_range;
	final private int m_rate;
	final private int m_ops;
	final private Random m_random;
	
	int m_nb_add;
	int m_nb_succ_add;
	int m_nb_remove;
	int m_nb_succ_remove;
	int m_nb_elements;
	int m_nb_aborts;
	boolean m_write;
	int m_last;
	
	public boolean isWriter = false;
	public boolean isReader = false;
	
	public ArrayList<OBNode> localadds = new ArrayList<OBNode>();
	
	int[] executionOperations = new int[20];
	int[] executionItems = new int[20];
	
	public QueueThread(IntQueue queue,int range,int rate,int ops) {
		m_queue = queue;
		m_range = range;
		m_rate = rate;
		m_ops = ops;
		m_nb_add = m_nb_succ_add = m_nb_remove = m_nb_succ_remove = m_nb_elements = 0;
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
			try {
				int nb_add = 0, nb_succ_add = 0, nb_remove = 0, nb_succ_remove = 0, nb_contains = 0;
				for (int c = 0; c < m_ops; c++) {
					if (executionOperations[0] == 1) {
						if (m_write) {
							m_last = executionItems[c];
							result = m_queue.add(m_last);
							if(result)
								m_write = false;
							if (phase == Benchmark.TEST_PHASE)
							{
								nb_add++;
								if(result == true)
									nb_succ_add++;
							}
						} else {
							if(m_queue.remove() != -1)
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
						m_queue.element();
						if (phase == Benchmark.TEST_PHASE)
							nb_contains++;
					}
				}
				m_queue.commit();
				m_nb_add += nb_add;
				m_nb_succ_add += nb_succ_add;
				m_nb_remove += nb_remove;
				m_nb_succ_remove += nb_succ_remove;
				m_nb_elements += nb_contains;
			} catch (AbortedException e) {
				flag = true;
				m_last = oldm_last;
				m_write = oldm_write;
				m_nb_aborts++;
				m_queue.abort();
			}
		}
	}
	
	public String getStats() {
		return "A=" + m_nb_add + ", SA=" + m_nb_succ_add + ", R=" + m_nb_remove + ", SR=" + m_nb_succ_remove + ", C=" + m_nb_elements
				+ ", Aborts=" + m_nb_aborts;
	}
	
}
