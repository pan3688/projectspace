package mpp.maps.open;

public class Window {

	public OBNode pred,curr;
	int position;
	
	public Window(OBNode myPred,OBNode myCurr) {
		pred = myPred;
		curr = myCurr;
	}
	
	public Window(OBNode myPred,OBNode myCurr, int pos) {
		pred = myPred;
		curr = myCurr;
		position = pos;
	}
}

