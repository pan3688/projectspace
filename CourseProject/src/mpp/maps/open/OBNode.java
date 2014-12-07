package mpp.maps.open;

import java.util.concurrent.atomic.AtomicInteger;

public class OBNode {
	public volatile int key;
	public volatile boolean marked;
	public volatile OBNode next;
	public volatile AtomicInteger lock = new AtomicInteger(0);
	public Object value;
	public volatile long lockHolder = -1;
	public int item;
	
	public OBNode(int key, int item, Object value) {
		this.key = key;
		this.value = value;
		this.item = item;
		marked = false;
	}
}
