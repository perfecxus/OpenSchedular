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
	
	/**
	 * When you need to create a scheduler which has jobs dependent on multiple jobs, then creation of a 
	 * common successor group has to be done. All jobs having same successor should use the same successor group.
	 * This method gives a key denoting the common output Q of all Jobs having same successor.
	 * @param noOfMembers : Total number of members/jobs having same successor job
	 * @return a String denoting Key of the common output Q.
	 */
	public static String createCommonSuccessorJobGroup(int noOfMembers){
		BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(noOfMembers);
		String newKey = "jobgroup_queue_" + multiJobOutputQueueCapacity.size();
		QueueListener.INSTANCE.setKey(queue, newKey);
		multiJobOutputQueueCapacity.putIfAbsent(newKey, noOfMembers);
		return newKey;
	}
	
	
	
}
