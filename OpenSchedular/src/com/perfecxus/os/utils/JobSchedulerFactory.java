package com.perfecxus.os.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.perfecxus.os.core.Job;
import com.perfecxus.os.core.JobAdapter;
import com.perfecxus.os.core.QueueListener;

public class JobSchedulerFactory {
	
	private static int queueCount=0;
	
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
