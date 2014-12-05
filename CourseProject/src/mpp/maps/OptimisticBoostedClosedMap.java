package mpp.maps;

import java.util.concurrent.atomic.AtomicInteger;

import mpp.exception.AbortedException;

public class OptimisticBoostedClosedMap implements IntMap<Integer,Object> {

	private static final float THRESHHOLD = 0.4f;
	final int PUT = 1;
	final int GET = 2;
	final int CONTAINS = 3;
	
	public BucketList<OBNode>[] bucket;
	public AtomicInteger bucketSize;
	public AtomicInteger setSize;
	
	public OptimisticBoostedClosedMap(int capacity) {
		// TODO Auto-generated constructor stub
		bucket = new BucketList[capacity];
		bucket[0] = new BucketList<>();
		bucketSize = new AtomicInteger(2);
		setSize = new AtomicInteger(0);
	}
	
	@Override
	public boolean put(Integer k, Object v) throws AbortedException {
		// TODO Auto-generated method stub
		int myBucket = k.hashCode() % bucketSize.get();
		
		BucketList<OBNode> b = getBucketList(myBucket);
		
		if(!b.add(new OBNode(k, v)))
			return false;
		
		int setSizeNow = setSize.getAndIncrement();
		int bucketSizeNow = bucketSize.get();
		if(setSizeNow / bucketSizeNow > THRESHHOLD)
			bucketSize.compareAndSet(bucketSizeNow, 2 * bucketSizeNow);
		
		return true;
	}
	
	private BucketList<OBNode> getBucketList(int myBucket){
		if(bucket[myBucket] == null)
			initializeBucket(myBucket);
		
		return bucket[myBucket];
	}
	
	private void initializeBucket(int myBucket){
		int parent = getParent(myBucket);
		if(bucket[parent] == null)
			initializeBucket(parent);
		BucketList<OBNode> b = bucket[parent].getSentinel(myBucket);
		if(b != null)
			bucket[myBucket] = b;
	}
	
	private int getParent(int myBucket){
		int parent = bucketSize.get();
		do{
			parent = parent >> 1;
		}while(parent > myBucket);
		parent = myBucket - parent;
		return parent;
	}
	
	@Override
	public Object get(Integer k) throws AbortedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean contains(Integer k) throws AbortedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean commit() throws AbortedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean abort() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nonTransactionalPut(Integer k, Object v) {
		// TODO Auto-generated method stub
		return false;
	}

}
