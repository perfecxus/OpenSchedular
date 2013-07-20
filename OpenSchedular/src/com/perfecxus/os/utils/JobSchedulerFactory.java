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

import com.perfecxus.os.core.Job;
import com.perfecxus.os.core.JobAdapter;
import com.perfecxus.os.core.QueueListener;

public class JobSchedulerFactory {
	
	private static int queueCount=0;
	
	/**
	 * @param jobName
	 * @param task
	 * @return
	 */
	public static Job createIndependentJob(String jobName,final Runnable task){
		BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(1);
		QueueListener.INSTANCE.setKey(queue, "Q_"+ ++queueCount);
		return new JobAdapter(null, jobName, queue) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};
	}
	
	/**
	 * @param jobName
	 * @param task
	 * @param commonOutputQKey
	 * @return
	 */
	public static Job createIndependentJobWithCommonSuccessor(String jobName,final Runnable task, String commonOutputQKey){
		return new JobAdapter(null, jobName, QueueListener.INSTANCE.getQueue(commonOutputQKey)) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};

	}
	
	/**
	 * @param jobName
	 * @param task
	 * @param dependentJob
	 * @return
	 */
	public static Job createDependentJob(String jobName,final Runnable task, Job dependentJob){
		BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(1);
		QueueListener.INSTANCE.setKey(queue, "Q_"+ ++queueCount);
		return new JobAdapter(Dependencies.setDependentJob(dependentJob), jobName, queue) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};
	}
	
	/**
	 * @param jobName
	 * @param task
	 * @param dependentJob
	 * @param commonOutputQKey
	 * @return
	 */
	public static Job createDependentJobWithCommonSuccessor(String jobName,final Runnable task,Job dependentJob, String commonOutputQKey){
	
		return new JobAdapter(Dependencies.setDependentJob(dependentJob), jobName,  QueueListener.INSTANCE.getQueue(commonOutputQKey)) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};
	}
	
	/**
	 * @param jobName
	 * @param task
	 * @param commonInputQKey
	 * @return
	 */
	public static Job createMultiTaskDependentJob(String jobName,final Runnable task, String commonInputQKey){
		BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(1);
		QueueListener.INSTANCE.setKey(queue, "Q_"+ ++queueCount);
		return new JobAdapter(QueueListener.INSTANCE.getQueue(commonInputQKey), jobName, queue) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};
	}
	
	/**
	 * @param jobName
	 * @param task
	 * @param commonInputQKey
	 * @param commonOutputQKey
	 * @return
	 */
	public static Job createMultiTaskDependentJobWithCommonSuccessor(String jobName,final Runnable task,String commonInputQKey, String commonOutputQKey){
		return new JobAdapter(QueueListener.INSTANCE.getQueue(commonInputQKey), jobName,  QueueListener.INSTANCE.getQueue(commonOutputQKey)) {
			
			@Override
			public void doWork() throws Exception {
				task.run();
				
			}
			
			@Override
			public <V extends Exception> void exceptionHandler(V e) {
				// TODO Auto-generated method stub
				
			}
		};
	}
}
