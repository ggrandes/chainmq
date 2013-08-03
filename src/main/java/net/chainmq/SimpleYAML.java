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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Simple YAML Emitter
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class SimpleYAML {
	/**
	 * Dump Lists
	 * 
	 * @param list
	 * @return
	 */
	public static String emitter(final Collection<String> list) {
		final StringBuilder sb = new StringBuilder();
		sb.append('-').append('-').append('-').append('\n');
		for (final String item : list) {
			sb.append('-').append(' ').append(item).append('\n');
		}
		return sb.toString();
	}

	/**
	 * Dump Associative arrays
	 * 
	 * @param map
	 * @return
	 */
	public static String emitter(final Map<String, Object> map) {
		final StringBuilder sb = new StringBuilder();
		sb.append('-').append('-').append('-').append('\n');
		for (final Entry<String, Object> item : map.entrySet()) {
			sb.append(item.getKey()).append(':').append(' ').append(item.getValue()).append('\n');
			Long.valueOf(0);
		}
		return sb.toString();
	}

	public static void main(final String[] args) {
		// List
		ArrayList<String> list = new ArrayList<String>();
		list.add("default");
		list.add("item2");
		System.out.println(emitter(list));
		// Map
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("default", 5.6f);
		map.put("item2", 9.3);
		map.put("item3", 49129310831L);
		map.put("item4", "test");
		System.out.println(emitter(map));
	}
}
