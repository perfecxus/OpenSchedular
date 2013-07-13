/*Copyright (c) 2013 Perfecxus Global
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
 * and associated documentation files (the "Software"), to deal in the Software without restriction, 
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, 
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 *  portions of the Software.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT 
 *  LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 *  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
 *  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
*/


package com.perfecxus.os.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


public enum QueueListener {
	INSTANCE;
	
	ConcurrentHashMap<String, BlockingQueue> chainedQueues = new ConcurrentHashMap<String, BlockingQueue>();
	List<QueueKey> keys = new ArrayList<QueueKey>();
	
	
	public void chainQueues(BlockingQueue<Boolean> queue1,BlockingQueue<Boolean> queue2){
		chainedQueues.putIfAbsent(getKey(queue1), queue2);
		System.out.println(chainedQueues);
	}
	
	public  void publishEvent(BlockingQueue<Boolean> queue1){
		//System.out.println("publishing event for q: " + getKey(queue1));
		
			BlockingQueue<Boolean> affectedQ =chainedQueues.get(getKey(queue1));
			if(affectedQ!=null){
				//System.out.println("publishing event: Changing queue: " +affectedQ + " current time: " +System.currentTimeMillis());
				affectedQ.clear();
				publishEvent(affectedQ);
			}

	}
	
	public String getKey(BlockingQueue<Boolean> queue){
		String resultKey=null;
		for(QueueKey q: keys)
		{
			if(q.queue.equals(queue)){
				resultKey = q.key;
				break;
			}
			
		}
		return resultKey;
	}
	
	public BlockingQueue<Boolean> getQueue(String key){
		BlockingQueue<Boolean> resultQ=null;
		for(QueueKey q: keys)
		{
			if(q.key.equals(key)){
				resultQ = q.queue;
				break;
			}
			
		}
		return resultQ;
	}
	
	public void setKey(BlockingQueue<Boolean> queue,String newKey){
		String oldKey =getKey(queue);
		if(oldKey==null){
			keys.add(new QueueKey(newKey,queue));
		}
		else if(oldKey.equals(newKey))
		{
			for(QueueKey queueKey : keys){
				if(oldKey.equals(queueKey.key)){
					queueKey.key = newKey;
					return;
				}
			}
		}
	}
	
	
	class QueueKey{
		 String key;
		 BlockingQueue<Boolean> queue;
		public QueueKey(String key, BlockingQueue<Boolean> queue) {
			super();
			this.key = key;
			this.queue = queue;
		}
		
	}
}
