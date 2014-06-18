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

import java.text.ParseException;

import org.apache.log4j.Logger;

/**
 * Simple Number Sequencer.
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class SequenceNumber {
	private static final Logger log = Logger.getLogger(SequenceNumber.class);
	private static final SequenceNumber singleton = new SequenceNumber();
	private volatile long counter = 0L;
	private volatile int overflows = 0;

	/**
	 * Return default instance of SequenceNumber
	 * 
	 * @return instance of SequenceNumber
	 */
	public static SequenceNumber getDefaultInstance() {
		return singleton;
	}

	/**
	 * Create new Number Sequencer
	 */
	public SequenceNumber() {
	}

	/**
	 * Get values between 0 and 9223372036854775807 in round-robin fashion.
	 * 
	 * First call return 1, when overflow return 0 and again starts in 1.
	 * 
	 * @return long value
	 */
	public final long nextLong() {
		final long value = ((++counter) & Long.MAX_VALUE);
		if (value == 0L) {
			++overflows;
			log.warn("Overflow counter incremented: " + overflows, new OverflowException());
		}
		return value;
	}

	public final int getOverflowCount() {
		return overflows;
	}

	public static class OverflowException extends Exception {
		private static final long serialVersionUID = 42L;
	}

	/**
	 * Simple test
	 */
	public static void main(final String[] args) throws Throwable {
		System.out.println("maxLong: " + Long.MAX_VALUE);
		SequenceNumber seq = SequenceNumber.getDefaultInstance();
		System.out.println("---");
		for (long i = 0; i < 5; i++) {
			System.out.println(seq.nextLong());
		}
		System.out.println("---");
		long begin = System.currentTimeMillis();
		for (long i = 0; i < 1e9; i++) {
			seq.nextLong();
		}
		long count = seq.nextLong();
		int overflows = seq.getOverflowCount();
		long diff = Math.max(1, System.currentTimeMillis() - begin);
		System.out.println("count=" + count + " time=" + diff + "ms " + (count / diff * 1000) + "seq/s"
				+ " overflows=" + overflows);
		new ParseException("fake", 0);
	}
}
