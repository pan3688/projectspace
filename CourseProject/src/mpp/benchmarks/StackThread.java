package mpp.benchmarks;

import java.util.ArrayList;
import java.util.Random;

import mpp.stack.IntStack;
import mpp.stack.OptimisticBoostedStack;

public class StackThread extends BenchmarkThread{

	final private IntStack m_stack;
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
	
	public ArrayList<OptimisticBoostedStack.OBNode> localAdds = new ArrayList<OptimisticBoostedStack.OBNode>();
	
	public boolean iswriter = false;
	
	public StackThread(IntStack stk, int range, int rate, int ops) {
		m_stack = stk;
		m_range = range;
		m_ops = ops;
		m_nb_add = m_nb_succ_add = m_nb_remove = m_nb_succ_remove = m_nb_contains = 0;
		m_rate = rate;
		m_write = true;
		m_random = new Random();
	}
	
}
