package mpp.maps;

import java.util.concurrent.atomic.AtomicMarkableReference;

public class BucketList<T>{
		public static final int HI_MASK = 0x80000000;
		public static final int MASK = 0x00FFFFFF;
		public OBNode head;
		public OBNode tail;
		
		public BucketList(){
			head = new OBNode(0,null);
			tail = new OBNode(Integer.MAX_VALUE,null);
			head.next = new AtomicMarkableReference<OBNode>(tail, false);
		}
		
		public BucketList(OBNode myHead){
			head = myHead;
		}
		
		public int makeOrdinaryKey(Integer i){
			int code = i.hashCode() & MASK;
			return Integer.reverse(code | HI_MASK);
		}
		
		private int makeSentinelKey(int key){
			return Integer.reverse(key & MASK);
		}
		
		public boolean contains(Integer i){
			int key = makeOrdinaryKey(i);
			Window window = find(head,key);
			OBNode curr = window.curr;
			return (curr.key == key);
		}
		
		public Window find(OBNode head,int key){
			OBNode pred = null,curr = null, succ = null;
			boolean[] marked = {false};
			boolean snip;
			retry:while(true){
				pred = head;
				curr = pred.next.getReference();
				while(true){
					succ = curr.next.get(marked);
					while(marked[0]){
						snip = pred.next.compareAndSet(curr, succ, false, false);
						if(!snip) continue retry;
						curr = succ;
						succ = curr.next.get(marked);
					}
					if(curr.key >= key)
						return new Window(pred,curr);
					
					pred = curr;
					curr = succ;
				}
			}
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
		
		/*public boolean remove(OBNode myNode){
			int key = myNode.key;
			
			boolean snip;
			
			while(true){
				Window window = find(head,key);
				OBNode pred = window.pred;
				OBNode curr = window.curr;
				if(curr.key != key){
					return false;
				}else{
					OBNode succ = curr.next.getReference();
					snip = curr.next.compareAndSet(succ, succ, false, true);
					if(!snip)
						continue;
					pred.next.compareAndSet(curr, succ, false, false);
					return true;
				}
			}
		}*/
		
		public BucketList<T> getSentinel(int index){
			
			int key = makeSentinelKey(index);
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
					
					if(curr.key == key){
						return new BucketList<T>(curr);
					}else{
						OBNode myNode = new OBNode(key, null);
						myNode.next.set(pred.next.getReference(), false);
						splice = pred.next.compareAndSet(curr, myNode, false, false);
						
						if(splice)
							return new BucketList<T>(myNode);
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