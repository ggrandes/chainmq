/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.javastack.chainmq;

import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * AsyncTask Tracker
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class AsyncTaskTracker {
	private static final Logger log = Logger.getLogger(AsyncTaskTracker.class);
	private final ScheduledExecutorService threadSchedPool; // Executors.newScheduledThreadPool(4);
	private final SequenceNumber seqReq = new SequenceNumber();
	private final HashSet<Long> seqAckPending = new HashSet<Long>();
	
	public AsyncTaskTracker(final ScheduledExecutorService threadSchedPool) {
		this.threadSchedPool = threadSchedPool;
	}

	/**
	 * Generate new task id and mark pending state
	 * 
	 * @return id
	 */
	public long newTaskId() {
		final long id = seqReq.nextLong();
		if (log.isDebugEnabled())
			log.debug("new task id=" + id);
		seqAckPending.add(Long.valueOf(id));
		return id;
	}

	/**
	 * Check if task is pending
	 * 
	 * @param id
	 * @return
	 */
	public boolean checkPendingId(final long id) {
		final boolean ret = seqAckPending.contains(Long.valueOf(id));
		if (log.isDebugEnabled())
			log.debug("checkPendingId id=" + id + " ret=" + ret);
		return ret;
	}

	/**
	 * Mark Task as Done
	 * 
	 * @param id
	 * @return true if task changed from pending to ack
	 */
	public boolean ackPendingId(final long id) {
		final boolean ret = seqAckPending.remove(Long.valueOf(id));
		if (log.isDebugEnabled())
			log.debug("ackPendingId id=" + id + " ret=" + ret);
		return ret;
	}

	/**
	 * Submit task for async processing
	 * 
	 * @param task
	 * @param delay time in millis
	 */
	public void submitAsyncTask(final AsyncTaskRunner task, final long delay) {
		if (log.isDebugEnabled())
			log.debug("submitAsyncTask id=" + task.sequence + " delay=" + delay);
		threadSchedPool.schedule(task, delay, TimeUnit.MILLISECONDS);
	}

	public void dumpStats() {
		 log.info("dumpStats() pending taskAck=" + seqAckPending.size());
	}
}
