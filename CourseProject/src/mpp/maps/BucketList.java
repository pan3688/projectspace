package mpp.maps;

import java.util.concurrent.atomic.AtomicMarkableReference;

public class BucketList<T>{
		public static final int HI_MASK = 0x80000000;
		public static final int MASK = 0x00FFFFFF;
		public static final int ONE = 0x00000001;
		public OBNode head;
		public OBNode tail;
		
		public BucketList(){
			int key = makeSentinelKey(0);
			head = new OBNode(key, 0, null);
			tail = new OBNode(Integer.MAX_VALUE, Integer.MAX_VALUE, null);
			head.next = tail;
		}
		
		public BucketList(OBNode myHead){
			head = myHead;
		}
		
		public static int makeOrdinaryKey(Integer i){
			/*int code = i.hashCode() & MASK;
			return Math.abs(Integer.reverse(code | HI_MASK));*/
			
			int code = i & MASK;
			int rev = Integer.reverse(code);
			code = rev >>> 1;
			return (code | ONE);
		}
		
		private int makeSentinelKey(int key){
			return Integer.reverse(key & MASK);
		}
		
		
		public Window find(OBNode head, int key, int item){
			OBNode pred = null,curr = null, succ = null;

			pred = head;
			curr = pred.next;
			while(curr.key <= key && curr.item != item){
				pred = curr;
				curr = curr.next;
			}
			
			return new Window(pred,curr);
		}
		
		public boolean add(OBNode myNode){
			
			int key = myNode.key;
			
			while(true){
				Window window = find(head, key, myNode.item);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
				
				if(curr.key == key){
					return false;
				}else{
					OBNode node = myNode;
					node.next = curr;
					pred.next = node;
					return true;
				}
			}
		}
		
		public BucketList<T> getSentinel(int index){
			
			int key = makeSentinelKey(index);
			boolean splice ;
			
			while(true){
				
				Window window = find(head, key, index);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
					
				if(curr.key == key){
					return new BucketList<T>(curr);
				}else{
						
					try{
						pred.lock.getAndIncrement();
						curr.lock.getAndIncrement();
						pred.lockHolder = Thread.currentThread().getId();
						curr.lockHolder = Thread.currentThread().getId();
						
						if(!pred.marked && !curr.marked && pred.next == curr){
							OBNode myNode = new OBNode(key, index, null);
							myNode.next = curr;
							pred.next = myNode;
							return new BucketList<T>(myNode);
						}
						else
							continue;
						
					}finally{
						pred.lock.getAndDecrement();
						curr.lock.getAndDecrement();
						pred.lockHolder = -1;
						curr.lockHolder = -1;
					}
				}
			}
		}
	}