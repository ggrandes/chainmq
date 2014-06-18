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

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;


import org.apache.log4j.Logger;
import org.javastack.chainmq.Job.JobState;

/**
 * Client Context
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class ClientContext {
	private static final Logger log = Logger.getLogger(ClientContext.class);
	private final ReservedJobsExpirer expirer;
	public final AsyncTaskTracker tracker;
	public final CopyOnWriteArraySet<String> watchedTubes = new CopyOnWriteArraySet<String>();
	private final TreeSet<Job> reservedJobs = new TreeSet<Job>(Job.reserveExpireComparator);
	public final ClientHandler clientHandler;
	String currentTube = Constants.DEFAULT_TUBE;

	public ClientContext(final ReservedJobsExpirer expirer, final ClientHandler clientHandler,
			final AsyncTaskTracker tracker) {
		this.expirer = expirer;
		this.clientHandler = clientHandler;
		this.tracker = tracker;
		addWatchedTube(currentTube);
	}

	public AsyncTaskTracker getTracker() {
		return tracker;
	}

	public String getCurrentTube() {
		return currentTube;
	}

	public void setCurrentTube(final String currentTube) {
		this.currentTube = currentTube;
	}

	public void addWatchedTube(final String tubeName) {
		watchedTubes.add(tubeName);
	}

	public Set<String> getWatchedTubes() {
		return Collections.unmodifiableSet(watchedTubes);
	}

	public void ignoreWatchedTube(final String tubeName) {
		watchedTubes.remove(tubeName);
	}

	public void jobReserve(final Job job) {
		if (log.isDebugEnabled())
			log.debug("Reserved job: " + job);
		job.setReserved(this);
		expirer.watch(this);
	}

	public synchronized Job jobReservedNearExpire() {
		if (reservedJobs.isEmpty())
			return null;
		final Job expire = reservedJobs.first();
		if (log.isTraceEnabled())
			log.trace("NearExpireJob: " + expire);
		return expire;
	}

	public synchronized void jobsFreeReserved() {
		for (final Job j : reservedJobs.toArray(new Job[0])) {
			if (log.isDebugEnabled())
				log.debug("FreeJob: " + j);
			if (j.getState() == JobState.RESERVED)
				j.setReady();
		}
	}

	public synchronized void addReserve(final Job job) {
		synchronized (job.tube) {
			reservedJobs.add(job);
		}
	}

	public synchronized void removeReserve(final Job job) {
		synchronized (job.tube) {
			reservedJobs.remove(job);
		}
	}

	public synchronized void freeResources() {
		jobsFreeReserved();
	}

}
