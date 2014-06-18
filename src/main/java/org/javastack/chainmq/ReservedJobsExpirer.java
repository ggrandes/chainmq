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

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.log4j.Logger;
import org.javastack.chainmq.Job.JobState;


/**
 * Expirer for Reserved Jobs
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class ReservedJobsExpirer implements Runnable {
	private static final Logger log = Logger.getLogger(ReservedJobsExpirer.class);
	private final Set<ClientContext> contextWatched = new LinkedHashSet<ClientContext>();

	public ReservedJobsExpirer() {
	}

	public void watch(final ClientContext context) {
		synchronized (contextWatched) {
			contextWatched.add(context);
		}
	}

	@Override
	public void run() {
		log.info("[" + Thread.currentThread().getName() + "] Thread start");
		try {
			final ClientContext[] type = new ClientContext[0];
			ClientContext[] contexts = null;
			Job job = null;
			while (true) {
				synchronized (contextWatched) {
					contexts = contextWatched.toArray(type);
				}
				for (final ClientContext context : contexts) {
					while ((job = context.jobReservedNearExpire()) != null) {
						if (job.getState() == JobState.RESERVED) {
							if (job.checkReserveExpired()) {
								if (log.isDebugEnabled())
									log.debug("Reserve expired job: " + job);
								job.setReady();
							} else {
								break; // Next Context
							}
						}
					}
					synchronized (contextWatched) {
						if (context.jobReservedNearExpire() == null)
							contextWatched.remove(context);
					}
				}
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			log.error("InterruptedException: " + e.toString(), e);
		} catch (Exception e) {
			log.error("Exception: " + e.toString(), e);
		} finally {
			log.info("[" + Thread.currentThread().getName() + "] Thread end");
		}
	}

	public void start() {
		final Thread cleanerThread = new Thread(this);
		cleanerThread.setName("ReservedJobsExpirer");
		cleanerThread.setDaemon(true);
		cleanerThread.start();
	}

}
