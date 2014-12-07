package mpp.maps.open;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

import mpp.benchmarks.OpenMapThread;
import mpp.maps.open.OptimisticBoostedOpenMap.ReadSetEntry;
import mpp.maps.open.OptimisticBoostedOpenMap.WriteSetEntry;

public class BucketListOpen<T>{
		
		public static final int HI_MASK = 0x80000000;
		public static final int MASK = 0x00FFFFFF;
		public OBNode head;
		public OBNode tail;
		public AtomicInteger size;
		public final int parentHash;
		
		public BucketListOpen(int hash){
			head = new OBNode(0,0,null);
			tail = new OBNode(Integer.MAX_VALUE,Integer.MAX_VALUE,null);
			head.next = tail;
			head.marked = false;
			bucketSize.set(0);
			parentHash = hash;
		}
		
		public BucketListOpen(OBNode myHead, int hash){
			head = myHead;
			parentHash = hash;
		}
		
		public int makeOrdinaryKey(int key){
			
			int code = key & MASK;
			return Integer.reverse(code | HI_MASK);
		}
		
		private int makeSentinelKey(int i, int bucketIndex){
			
			int key = bucketIndex;
			if(i == 0){
				key = OptimisticBoostedOpenMap.hash0(bucketIndex);
			}
			else if(i == 1){
				key = OptimisticBoostedOpenMap.hash1(bucketIndex);
			}
				
			return Integer.reverse(key & MASK);
		}
		
		public boolean contains(int item, int itemKey){
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			
			//convert itemKey to split order key before searching
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head, key, item);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.item == item && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, false));
				return true;
			}
			else
				return false;
		}
		
		public Object get(int item, int itemKey){
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			
			//convert itemKey to split order key before searching
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head, key, item);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.item == item && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, false));
				return curr.value;
			}
			else
				return null;
		}		
		
		
		public Window find(OBNode head, int key, int item){
			OBNode pred = null,curr = null, succ = null;

			pred = head;
			curr = pred.next;
			int pos = 0;
			while(curr.key <= key && curr.item != item){
				pred = curr;
				curr = curr.next;
				pos++;
			}
			
			return new Window(pred,curr,pos);
		}
		
		public boolean add(int item, int itemKey, Object value){
			
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			TreeMap<Integer, WriteSetEntry> writeset = ((OpenMapThread) Thread.currentThread()).list_writeset;
			
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head, key, item);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.item == item && !curr.marked)
				return false;
			else{
				readset.add(new ReadSetEntry(pred, curr, true));
				writeset.put(curr.item, new WriteSetEntry(pred, curr, OptimisticBoostedOpenMap.PUT, key, item, value, parentHash));
				return true;
			}
		}
		
		public Object remove(int item, int itemKey){
			
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			TreeMap<Integer, WriteSetEntry> writeset = ((OpenMapThread) Thread.currentThread()).list_writeset;
			
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head, key, item);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.item == item && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, true));
				writeset.put(curr.item, new WriteSetEntry(pred, curr, OptimisticBoostedOpenMap.REMOVE, curr.key, curr.item, curr.value, parentHash));
				return curr.value;
			}
			else
				return null;
		}
		
		public OBNode getFirst(){
			
			OBNode toReturn = head.next;
			
			if(toReturn.key == makeSentinelKey(parentHash,toReturn.item))
				return null;
			else
				return toReturn;
			
		}
		
		//To add sentinel directly to the object. No entries will be added to read/write sets
		public BucketListOpen<T> getSentinel(int i, int bucketIndex){

			int key = makeSentinelKey(i, bucketIndex);
		
			while(true){
				Window window = find(head, key, bucketIndex);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
				int pos = window.position;

				if(curr.key == key && !curr.marked){
					return new BucketListOpen<T>(curr, i);
				}else{ 

					OBNode myNode = null;
					try{
						pred.lock.getAndIncrement();
						curr.lock.getAndIncrement();
						pred.lockHolder = Thread.currentThread().getId();
						curr.lockHolder = Thread.currentThread().getId();

						if(!pred.marked && !curr.marked && pred.next == curr){
							myNode = new OBNode(key, bucketIndex, null);
							myNode.next = curr;
							pred.next = myNode;
							BucketListOpen<T> newBucket = new BucketListOpen<T>(myNode, i); 
							newBucket.size.set(this.size.get() - pos);
							this.size.set(pos);
							return newBucket;
						}
						else
							continue;
					}
					finally{
						pred.lock.getAndDecrement();
						curr.lock.getAndDecrement();
						pred.lockHolder = -1;
						curr.lockHolder = -1;
					}
				}
			}
		}
	}
