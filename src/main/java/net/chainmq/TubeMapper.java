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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Tube Mapper
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class TubeMapper implements Runnable {
	private static final Logger log = Logger.getLogger(Tube.class);
	private static final TubeMapper singleton = new TubeMapper();
	private final LinkedHashMap<String, Tube> tubes = new LinkedHashMap<String, Tube>();
	final SequenceNumber seq = new SequenceNumber();
	final Map<Long, Job> jobsGlobalByID = Collections.synchronizedMap(new HashMap<Long, Job>());

	static {
		getInstance().getTubeOrCreate(Constants.DEFAULT_TUBE);
	}

	public static TubeMapper getInstance() {
		return singleton;
	}

	TubeMapper() {
		start();
	}

	public synchronized Tube getTubeOrCreate(final String name) {
		Tube tube = tubes.get(name);
		if (tube == null) {
			tube = new Tube(seq, jobsGlobalByID, name);
			tubes.put(name, tube);
		}
		return tube;
	}

	public synchronized Tube getTubeIfExist(final String name) {
		return tubes.get(name);
	}

	public synchronized List<String> getTubeList() {
		return new ArrayList<String>(tubes.keySet());
	}

	@Override
	public void run() {
		log.info("[" + Thread.currentThread().getName() + "] Thread start");
		try {
			while (true) {
				boolean doWait = true;
				synchronized (this) {
					for (final Tube tube : tubes.values()) {
						if (!tube.processSchedQueue()) {
							doWait = false;
							continue;
						}
					}
				}
				if (doWait)
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

	private synchronized final void start() {
		final Thread cleanerThread = new Thread(this);
		cleanerThread.setName("TubeMapper");
		cleanerThread.setDaemon(true);
		cleanerThread.start();
	}

}
