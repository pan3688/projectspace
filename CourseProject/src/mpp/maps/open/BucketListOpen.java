package mpp.maps.open;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

import mpp.benchmarks.OpenMapThread;
import mpp.maps.open.OptimisticBoostedOpenMap.ReadSetEntry;

public class BucketListOpen<T>{
		
		public static final int HI_MASK = 0x80000000;
		public static final int MASK = 0x00FFFFFF;
		public OBNode head;
		public OBNode tail;
		public AtomicInteger bucketSize;
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
		
		public boolean contains(int itemKey){
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			
			//convert itemKey to split order key before searching
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head,key);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.key == key && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, false));
				return true;
			}
			else
				return false;
		}
		
		public Object get(int itemKey){
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			
			//convert itemKey to split order key before searching
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head,key);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.key == key && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, false));
				return curr.value;
			}
			else
				return null;
		}		
		
		
		public Window find(OBNode head,int key){
			OBNode pred = null,curr = null, succ = null;

			pred = head;
			curr = pred.next;
			while(curr.key < key){
				pred = curr;
				curr = curr.next;
			}
			
			return new Window(pred,curr);
		}
		
		public boolean add(OBNode myNode){
			int key = myNode.key;
			
			while(true){
				Window window = find(head,key);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
				
				if(curr.key == key){
					return false;
				}else{
					OBNode node = myNode;
					node.next = new AtomicMarkableReference<OBNode>(curr, false);
					
					if(pred.next.compareAndSet(curr, node, false, false)){
						return true;
					}
				}
			}
		}
		
		public boolean remove(int itemKey){
			
			ArrayList<ReadSetEntry> readset = ((OpenMapThread)Thread.currentThread()).list_readset;
			
			int key = makeOrdinaryKey(itemKey);
			Window window = find(head,key);
			OBNode pred = window.pred;
			OBNode curr = window.curr;
			if(curr.key == key && !curr.marked){
				readset.add(new ReadSetEntry(pred, curr, false));
				return true;
			}
			else
				return false;
		}
		
		//To add sentinel directly to the object. No entries will be added to read/write sets
		public BucketListOpen<T> getSentinel(int i, int bucketIndex){
			
			int key = makeSentinelKey(i, bucketIndex);
			boolean splice ;
			
			while(true){
				
				Window window = find(head,key);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
				
				try{
					pred.lock.getAndIncrement();
					curr.lock.getAndIncrement();
					pred.lockHolder = Thread.currentThread().getId();
					curr.lockHolder = Thread.currentThread().getId();
					
				
					if(curr.key == key && !curr.marked){
						return new BucketListOpen<T>(curr, i);
					}else{
						OBNode myNode = new OBNode(key, bucketIndex, null);
						myNode.next.set(pred.next.getReference(), false);
						splice = pred.next.compareAndSet(curr, myNode, false, false);

						if(splice)
							return new BucketListOpen<T>(myNode, i);
						else
							continue;
					}
				}finally{
						pred.lock.getAndDecrement();
						curr.lock.getAndDecrement();
						pred.lockHolder = -1;
						curr.lockHolder = -1;
					}
				}
				
			}
	}
