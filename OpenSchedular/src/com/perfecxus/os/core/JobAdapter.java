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

import java.util.concurrent.BlockingQueue;

public abstract class JobAdapter implements Job {
	
	protected String jobName;
	private BlockingQueue<Boolean> inputQueue;
	private BlockingQueue<Boolean> outputQueue;
	private int numberOfInputJobs;
	
	
	public JobAdapter(BlockingQueue<Boolean> inputQueue, String jobName,
			BlockingQueue<Boolean> outputQueue) {
		this(inputQueue, 1, jobName, outputQueue);
	}

	public JobAdapter(BlockingQueue<Boolean> inputQueue, int numberOfInputJobs,String jobName,
			BlockingQueue<Boolean> outputQueue) {
		super();
		this.numberOfInputJobs = numberOfInputJobs;
		this.inputQueue = inputQueue;
		this.jobName = jobName;
		this.outputQueue = outputQueue;
		if(this.inputQueue!=null)
			QueueListener.INSTANCE.chainQueues(inputQueue, outputQueue);
	}
	@Override
	public void run() {
		try{
		outputQueue.clear();
		
			if(inputQueue==null || consume()){
			synchronized (outputQueue) {
				doWork();
				outputQueue.put(true);
				QueueListener.INSTANCE.publishEvent(outputQueue);
			}
				
			}
		}catch(Exception e){
			try {
				outputQueue.put(false);
				QueueListener.INSTANCE.publishEvent(outputQueue);

			} catch (InterruptedException e1) {
			}
			exceptionHandler(e);
		}
		
	}

	private boolean consume() throws InterruptedException {
		Boolean take = null;
		for(int counter=0;counter<numberOfInputJobs;counter++){
			if(take==null){
			take = inputQueue.take();	
			}
			else{
			take = take & inputQueue.take();
			}
		}
		if(!take){
			System.out.println("Since previous job didnot complete succesfully so, Escaping job" +jobName);
			return false;
		}
		return true;
	}

	public abstract <V extends Exception> void exceptionHandler(V e) ;

	public BlockingQueue<Boolean> getOutputQueue() {
		return outputQueue;
	}

}
