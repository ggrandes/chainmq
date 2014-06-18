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

/**
 * AsyncTask
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class AsyncTask {
	final long sequence;
	final ClientHandler clientHandler;
	final AsyncTaskTracker tracker;

	public AsyncTask(final long sequence, final ClientHandler clientHandler) {
		this.sequence = sequence;
		this.clientHandler = clientHandler;
		this.tracker = clientHandler.getContext().getTracker();
	}

	/**
	 * Mark Task as done
	 * 
	 * @return
	 */
	public final boolean taskMarkDone() {
		return tracker.ackPendingId(sequence);
	}

	/**
	 * Check if Task is done
	 * 
	 * @return
	 */
	public final boolean taskIsDone() {
		return !tracker.checkPendingId(sequence);
	}
}
