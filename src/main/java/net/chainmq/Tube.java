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
package net.chainmq;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;

/**
 * Tube
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class Tube {
	private static final Logger log = Logger.getLogger(Tube.class);
	final SequenceNumber seq;
	final String name;
	final Map<Long, Job> jobsByID;
	final TreeSet<Job> jobsByPrio = new TreeSet<Job>(Job.priorityComparator);
	final TreeSet<Job> jobsBySched = new TreeSet<Job>(Job.readyTimeComparator);
	final LinkedHashSet<Job> jobsBuried = new LinkedHashSet<Job>();
	final HashSet<Job> jobsReserved = new HashSet<Job>();
	final ArrayDeque<AsyncUpdateCallBack> notifyQueue = new ArrayDeque<AsyncUpdateCallBack>();
	long delayed = 0;
	long delayedUntil = 0;

	public Tube(final SequenceNumber seq, final Map<Long, Job> jobsGlobalByID, final String name) {
		this.seq = seq;
		this.jobsByID = jobsGlobalByID;
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public Job newJob(final long prio, final long delay, final long ttr, final byte[] data) {
		final long id = seq.nextLong();
		return new Job(this, id, prio, delay, ttr, data);
	}

	public synchronized Job peek(final long id) {
		return jobsByID.get(Long.valueOf(id));
	}

	public synchronized Job get() {
		if (isPaused())
			return null;
		return jobsByPrio.pollFirst();
	}

	public synchronized Job peek() {
		if (jobsByPrio.isEmpty())
			return null;
		return jobsByPrio.first();
	}

	public synchronized Job peekDelayed() {
		if (jobsBySched.isEmpty())
			return null;
		return jobsBySched.first();
	}

	public synchronized Job peekBuried() {
		if (jobsBuried.isEmpty())
			return null;
		final Iterator<Job> i = jobsBuried.iterator();
		if (i.hasNext()) {
			return i.next();
		}
		return null;
	}

	public synchronized long kick(final long count) {
		long kicked = 0;
		if (!jobsBuried.isEmpty()) {
			// If buried jobs, kicks only on Burieds
			final Iterator<Job> i = jobsBuried.iterator();
			while (i.hasNext()) {
				final Job job = i.next();
				i.remove();
				job.setReady();
				if (++kicked >= count)
					return kicked;
			}
		} else if (!jobsBySched.isEmpty()) {
			// If no buried, kicks on Delayed
			final Iterator<Job> i = jobsBySched.iterator();
			while (i.hasNext()) {
				final Job job = i.next();
				i.remove();
				job.setReady();
				if (++kicked >= count)
					return kicked;
			}
		}
		return kicked;
	}

	public synchronized void pushCallback(final AsyncUpdateCallBack notify) {
		notifyQueue.addLast(notify);
	}

	public static abstract class AsyncUpdateCallBack extends AsyncTask {
		public AsyncUpdateCallBack(final long sequence, final ClientHandler clientHandler) {
			super(sequence, clientHandler);
		}

		public abstract boolean newData(final Tube tube);
	}

	synchronized boolean processSchedQueue() {
		int maxExpunge = 100;
		boolean expunged = false;
		while (!jobsBySched.isEmpty() && (--maxExpunge > 0)) {
			final Job job = jobsBySched.first();
			final long now = System.currentTimeMillis();
			if (job.readyTime <= now) {
				log.info("Tube: <" + getName() + "> Scheduled job ready:" + job);
				job.setReady();
				expunged = true;
				continue;
			}
			break;
		}
		// Notify after tube pause
		resumeIfCan();
		return !expunged;
	}

	/**
	 * Delay reserves
	 * 
	 * @param delay
	 *            time in millis
	 */
	public synchronized void pause(final long delay) {
		delayed = Math.max(1000, delay);
		delayedUntil = System.currentTimeMillis() + delayed;
		log.info("Tube: <" + getName() + "> Paused " + delayed + "ms until: " + delayedUntil);
	}

	private final void resumeIfCan() {
		if (!wasPaused())
			return;
		if (isPaused())
			return;
		delayed = 0;
		delayedUntil = 0;
		log.info("Tube: <" + getName() + "> Resumed");
		processNotifyQueue();
	}

	private final boolean wasPaused() {
		return (delayedUntil > 0);
	}

	private final boolean isPaused() {
		if (delayedUntil > 0) {
			final long now = System.currentTimeMillis();
			if (delayedUntil > now) {
				return true;
			}
		}
		return false;
	}

	private final void processNotifyQueue() {
		if (isPaused())
			return;
		while (true) {
			final AsyncUpdateCallBack notify = notifyQueue.pollFirst();
			if (notify != null) {
				if (!notify.newData(this))
					continue;
			}
			break;
		}
	}

	public synchronized void addJob(final Job job) {
		jobsByID.put(Long.valueOf(job.id), job);
	}

	public synchronized void removeJob(final Job job) {
		jobsByID.remove(Long.valueOf(job.id));
	}

	public synchronized void addReady(final Job job) {
		jobsByPrio.add(job);
		processNotifyQueue();
	}

	public synchronized void removeReady(final Job job) {
		jobsByPrio.remove(job);
	}

	public synchronized void addReserve(final Job job) {
		jobsReserved.add(job);
	}

	public synchronized void removeReserve(final Job job) {
		jobsReserved.remove(job);
	}

	public synchronized void addBuried(final Job job) {
		jobsBuried.add(job);
	}

	public synchronized void removeBuried(final Job job) {
		jobsBuried.remove(job);
	}

	public synchronized void addDelayed(final Job job) {
		jobsBySched.add(job);
	}

	public synchronized void removeDelayed(final Job job) {
		jobsBySched.remove(job);
	}

	public synchronized Map<String, Object> getStats() {
		final Map<String, Object> map = new LinkedHashMap<String, Object>();
		//
		map.put("name", name);
		map.put("current-jobs-urgent", Integer.toString(countJobsUrgent()));
		map.put("current-jobs-ready", Integer.toString(jobsByPrio.size()));
		map.put("current-jobs-reserved", Integer.toString(jobsReserved.size()));
		map.put("current-jobs-delayed", Integer.toString(jobsBySched.size()));
		map.put("current-jobs-buried", Integer.toString(jobsBuried.size()));
		map.put("total-jobs", Integer.toString(jobsByID.size()));
		// TODO
		// map.put("current-using", null);
		// map.put("current-waiting", null);
		// map.put("current-watching", null);
		map.put("pause", Long.toString(delayed / 1000));
		// map.put("cmd-delete", null);
		// map.put("cmd-pause-tube", null);
		map.put("pause-time-left", Long.toString(Math.max(0, (delayedUntil - System.currentTimeMillis())) / 1000));
		return map;
	}

	private final int countJobsUrgent() {
		final Iterator<Job> i = jobsByPrio.iterator();
		int count = 0;
		while (i.hasNext()) {
			final Job j = i.next();
			if (j.prio < 1024) {
				count++;
			} else {
				break;
			}
		}
		return count;
	}

}
