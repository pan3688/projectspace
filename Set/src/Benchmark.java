/**
 * @author Pascal Felber
 * @since 0.1
 */


public interface Benchmark {

	public static final int WARMUP_PHASE = 1;
	public static final int TEST_PHASE = 2;
	public static final int SHUTDOWN_PHASE = 3;

	public void init(String[] args, int nb_threads);

	public BenchmarkThread createThread(int i, int nb);

	public String getStats(BenchmarkThread[] threads);
}
