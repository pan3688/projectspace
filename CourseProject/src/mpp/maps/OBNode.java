package mpp.maps;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class OBNode {
	public int key;
	public volatile boolean marked;
	public volatile AtomicMarkableReference<OBNode> next;
	public volatile AtomicInteger lock = new AtomicInteger(0);
	public Object value;
	public volatile long lockHolder = -1;
	
	public OBNode(int key,Object value) {
		this.key = key;
		this.value = value;
		marked = false;
	}
}
