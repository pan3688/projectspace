package mpp.benchmarks;

import java.util.ArrayList;
import java.util.List;

public class OTBThread extends Thread {

	public boolean isWriter = false;
	public boolean isReader = false;
	
	public List localadds = new ArrayList<OTBThread>();
	
}
