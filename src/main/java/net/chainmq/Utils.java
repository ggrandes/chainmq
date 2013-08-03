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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Utils
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class Utils {
	private static final Logger log = Logger.getLogger(Utils.class);

	public static int getPid() {
		int pid = 0;
		final File f = new File("/proc/self");
		try {
			if (f.exists())
				pid = Integer.parseInt(f.getCanonicalFile().getName());
		} catch (Exception e) {
			log.error("Exception: " + e.toString(), e);
		}
		if (pid == 0) {
			// something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
			final String jvmName = ManagementFactory.getRuntimeMXBean()
					.getName();
			final int index = jvmName.indexOf('@');

			try {
				pid = Integer.parseInt(jvmName.substring(0, index));
			} catch (Exception e) {
				log.error("Exception: " + e.toString(), e);
			}
		}
		return pid;
	}

	public static String getHostname() {
		try {
			return java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			log.error("Exception: " + e.toString(), e);
		}
		return "localhost";
	}

	public static long parseLong(final String n, final long maxValue) {
		try {
			final long l = Long.valueOf(n);
			if ((l >= 0) && (l <= maxValue))
				return l;
		} catch (Exception e) {
			log.error("Exception: " + e.toString(), e);
		}
		return -1;
	}

	public static int parseInteger(final String n, final int maxValue) {
		try {
			final int i = Integer.valueOf(n);
			if ((i >= 0) && (i <= maxValue))
				return i;
		} catch (Exception e) {
			log.error("Exception: " + e.toString(), e);
		}
		return -1;
	}

	public static int parseInteger(final String n) {
		return parseInteger(n, Integer.MAX_VALUE);
	}

	public static List<String> parseTokens(final String in) {
		try {
			final int[] seps = new int[Constants.REQUEST_MAX_TOKENS + 2];
			final int len = in.length();
			int j = 1;
			seps[0] = -1;
			for (int i = 0; i < len; i++) {
				final char c = in.charAt(i);
				if (c == ' ') {
					if (j < seps.length)
						seps[j++] = i;
				}
			}
			if (j < seps.length)
				seps[j++] = len;
			final String[] toks = new String[j - 1];
			for (int i = 1; i < j; i++) {
				toks[i - 1] = in.substring(seps[i - 1] + 1, seps[i]);
			}
			return Arrays.asList(toks);
		} catch (Exception e) {
			log.error("Exception " + e.toString(), e);
		}
		return Collections.emptyList();
	}
	
	public static void main(String[] args) {
		System.out.println(getPid());
		System.out.println(getHostname());
		System.out.println(System.getProperty("os.name"));
		System.out.println(System.getProperty("os.version"));
	}

}
