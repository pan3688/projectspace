/**
 * @author Pascal Felber
 * @since 0.1
 */
package mpp.benchmarks;

public class BenchmarkThread extends java.lang.Thread {

	volatile private int m_phase;
	private int m_steps;
	volatile private int delayCount;
	
	public BenchmarkThread() {
		m_phase = Benchmark.WARMUP_PHASE;
		m_steps = 0;
	}

	public void setPhase(int phase) {
		m_phase = phase;
	}

	public int getSteps() {
		return m_steps;
	}

	public void run() {
		while (m_phase == Benchmark.WARMUP_PHASE) {
			step(Benchmark.WARMUP_PHASE);
			delay();
		}
		while (m_phase == Benchmark.TEST_PHASE) {
			step(Benchmark.TEST_PHASE);
			m_steps++;
			delay();
		}
	}

	protected void step(int phase){}

	public String getStats(){return null;}
	
	private void delay()
	{
		for(delayCount=0;delayCount<256;delayCount++)
			;
	}
}
