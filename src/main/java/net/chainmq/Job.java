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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Job structure
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class Job {
	public static final PriorityComparator priorityComparator = new PriorityComparator();
	public static final ReadyTimeComparator readyTimeComparator = new ReadyTimeComparator();
	public static final ReserveExpireComparator reserveExpireComparator = new ReserveExpireComparator();
	//
	public transient final JobStats stats = new JobStats();
	public transient final Tube tube;
	public transient ClientContext clientContext = null;
	public transient long reserveExpire;
	public transient JobState state = JobState.NEW;
	public final long id;
	public final long created;
	public long prio;
	public long delay;
	public long readyTime;
	public final long ttr;
	public final byte[] body;

	public Job(final Tube tube, final long id, final long prio, final long delay, final long ttr,
			final byte[] body) {
		this.tube = tube;
		this.id = id & 0xFFFFFFFF; // Uint32
		this.created = System.currentTimeMillis();
		this.prio = prio & 0xFFFFFFFF; // Uint32
		this.delay = delay;
		this.readyTime = fromDelayToReadyTime(delay);
		this.ttr = Math.max(1, ttr);
		this.body = body;
	}

	private void setPrio(final long prio) {
		this.prio = prio;
	}

	private void setDelay(final long delay) {
		this.delay = delay;
		this.readyTime = fromDelayToReadyTime(delay);
	}

	public synchronized JobState getState() {
		return state;
	}

	private long fromDelayToReadyTime(final long delay) {
		return (System.currentTimeMillis() + (Math.max(0, delay) * 1000));
	}

	private void updateReserveExpire() {
		reserveExpire = (System.currentTimeMillis() + (ttr * 1000));
	}

	public synchronized boolean checkReserveExpired() {
		return (System.currentTimeMillis() >= reserveExpire);
	}

	public String toString() {
		return getStats().toString();
	}

	public synchronized void doNew() {
		// Source can be: new
		switch (state) {
		case NEW:
			break;
		default:
			return;
		}
		if (delay > 0) {
			setDelayed(delay);
		} else {
			setReady();
		}
	}

	public synchronized void doRelease(final long prio, final long delay) {
		// Source can be: reserved
		switch (state) {
		case RESERVED:
			stats.releases++;
			break;
		default:
			return;
		}
		setPrio(prio);
		if (delay > 0) {
			setDelayed(delay);
		} else {
			setReady();
		}
	}

	public synchronized boolean doKick() {
		// Source can be: buried/delayed
		switch (state) {
		case BURIED:
		case DELAYED:
			stats.kicks++;
			break;
		default:
			return false;
		}
		setReady();
		return true;
	}

	public synchronized void setReady() {
		// Source can be: new/reserved/buried/delayed
		switch (state) {
		case NEW:
			tube.addJob(this);
			break;
		case RESERVED:
			tube.removeReserve(this);
			clientContext.removeReserve(this);
			clientContext = null;
			stats.timeouts++;
			break;
		case BURIED:
			tube.removeBuried(this);
			break;
		case DELAYED:
			tube.removeDelayed(this);
			break;
		default:
			return;
		}
		state = JobState.READY;
		tube.addReady(this);
	}

	public synchronized boolean isReserved() {
		return (state == JobState.RESERVED);
	}

	public synchronized boolean isReserved(final ClientContext clientContext) {
		return ((state == JobState.RESERVED) && (this.clientContext == clientContext));
	}

	public synchronized void setReserved(final ClientContext clientContext) {
		// Source can be: ready/reserved (touch)
		switch (state) {
		case RESERVED:
			stats.reserves++;
			clientContext.removeReserve(this);
			break;
		case READY:
			this.clientContext = clientContext;
			break;
		default:
			return;
		}
		state = JobState.RESERVED;
		updateReserveExpire();
		clientContext.addReserve(this);
		tube.addReserve(this);
	}

	public synchronized void setBuried(final long prio) {
		// Source can be: reserved
		switch (state) {
		case RESERVED:
			stats.buries++;
			clientContext.removeReserve(this);
			clientContext = null;
			break;
		default:
			return;
		}
		state = JobState.BURIED;
		setPrio(prio);
		tube.addBuried(this);
	}

	public synchronized void setDelayed(final long delay) {
		// Source can be: new/reserved
		switch (state) {
		case NEW:
			tube.addJob(this);
			break;
		case RESERVED:
			tube.removeReserve(this);
			clientContext.removeReserve(this);
			clientContext = null;
			break;
		default:
			return;
		}
		state = JobState.DELAYED;
		setDelay(delay);
		tube.addDelayed(this);
	}

	public synchronized void setDeleted() {
		// Source can be: ready/reserved-by-owner/buried/delayed
		switch (state) {
		case READY:
			tube.removeReady(this);
			break;
		case RESERVED:
			tube.removeReserve(this);
			clientContext.removeReserve(this);
			clientContext = null;
			break;
		case BURIED:
			tube.removeBuried(this);
			break;
		case DELAYED:
			tube.removeDelayed(this);
			break;
		default:
			return;
		}
		state = JobState.DELETED;
		tube.removeJob(this);
	}

	public synchronized Map<String, Object> getStats() {
		final long now = System.currentTimeMillis();
		final Map<String, Object> map = new LinkedHashMap<String, Object>();
		long timeLeft = 0;
		if (state == JobState.RESERVED) {
			timeLeft = (reserveExpire - now);
		} else if (state == JobState.DELAYED) {
			timeLeft = (readyTime - now);
		}
		map.put("id", Long.toString(id));
		map.put("tube", tube.getName());
		map.put("state", JobState.humanState(state));
		map.put("pri", Long.toString(prio));
		map.put("age", Long.toString((now - created) / 1000));
		map.put("time-left", Long.toString(Math.max(timeLeft, 0) / 1000));
		// TODO
		map.put("file", "0");
		map.put("reserves", Integer.toString(stats.reserves));
		map.put("timeouts", Integer.toString(stats.timeouts));
		map.put("releases", Integer.toString(stats.releases));
		map.put("buries", Integer.toString(stats.buries));
		map.put("kicks", Integer.toString(stats.kicks));
		return map;
	}

	public static class PriorityComparator implements Comparator<Job> {
		@Override
		public int compare(final Job o1, final Job o2) {
			if (o1.prio < o2.prio)
				return -1;
			if (o1.prio > o2.prio)
				return 1;
			if (o1.id < o2.id)
				return -1;
			if (o1.id > o2.id)
				return 1;
			return 0;
		}

	}

	public static class ReadyTimeComparator implements Comparator<Job> {
		@Override
		public int compare(final Job o1, final Job o2) {
			if (o1.readyTime < o2.readyTime)
				return -1;
			if (o1.readyTime > o2.readyTime)
				return 1;
			if (o1.id < o2.id)
				return -1;
			if (o1.id > o2.id)
				return 1;
			return 0;
		}

	}

	public static class ReserveExpireComparator implements Comparator<Job> {
		@Override
		public int compare(final Job o1, final Job o2) {
			if (o1.reserveExpire < o2.reserveExpire)
				return -1;
			if (o1.reserveExpire > o2.reserveExpire)
				return 1;
			if (o1.id < o2.id)
				return -1;
			if (o1.id > o2.id)
				return 1;
			return 0;
		}

	}

	public static enum JobState {
		NEW, READY, RESERVED, BURIED, DELAYED, DELETED;
		public static final String humanState(final JobState state) {
			switch (state) {
			case READY:
				return "ready";
			case RESERVED:
				return "reserved";
			case BURIED:
				return "buried";
			case DELAYED:
				return "delayed";
			}
			return "invalid";
		}
	}

	public static class JobStats {
		public volatile int reserves = 0;
		public volatile int timeouts = 0;
		public volatile int releases = 0;
		public volatile int buries = 0;
		public volatile int kicks = 0;
	}
}
