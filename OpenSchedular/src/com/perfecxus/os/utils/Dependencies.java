package com.perfecxus.os.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.perfecxus.os.core.Job;
import com.perfecxus.os.core.JobAdapter;
import com.perfecxus.os.core.QueueListener;

public class Dependencies {

	
	
	static ConcurrentHashMap<String, Integer> multiJobOutputQueueCapacity = new ConcurrentHashMap<>();
	
	protected static BlockingQueue<Boolean> setDependentJob(Job job){
		return ((JobAdapter)job).getOutputQueue();
	}
	
	public static String createCommonSuccessorJobGroup(int noOfMembers){
		BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(noOfMembers);
		String newKey = "jobgroup_queue_" + multiJobOutputQueueCapacity.size();
		QueueListener.INSTANCE.setKey(queue, newKey);
		multiJobOutputQueueCapacity.putIfAbsent(newKey, noOfMembers);
		return newKey;
	}
	
	
	
}
