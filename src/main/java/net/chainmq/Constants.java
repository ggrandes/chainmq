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

/**
 * Constants
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class Constants {
	public static final int PROTO_READ_LINE = 0;
	public static final int PROTO_READ_BODY = 1;

	public static final int REQUEST_MAX_TOKENS = 5;
	public static final long MAX_INT_32BITS = 4294967295L;

	public static final int SAFETY_MARGIN = 1000; // 1 second (millis)
	
	public static final String CRLF = "\r\n";

	public static final String DEFAULT_TUBE = "default";

	// Errors
	public static final String ERROR_OUT_OF_MEMORY = "OUT_OF_MEMORY";
	public static final String ERROR_INTERNAL_ERROR = "INTERNAL_ERROR";
	public static final String ERROR_BAD_FORMAT = "BAD_FORMAT";
	public static final String ERROR_UNKNOWN_COMMAND = "UNKNOWN_COMMAND";
	//
	public static final String ERROR_NOT_FOUND = "NOT_FOUND";
	public static final String ERROR_EXPECTED_CRLF = "EXPECTED_CRLF";
	public static final String ERROR_JOB_TOO_BIG = "JOB_TOO_BIG";
	public static final String ERROR_DRAINING = "DRAINING";
	//
	public static final String ERROR_DEADLINE_SOON = "DEADLINE_SOON";
	public static final String ERROR_TIMED_OUT = "TIMED_OUT";
	// Custom Error
	public static final String ERROR_UNIMPLEMENTED_COMMAND = "UNIMPLEMENTED_COMMAND";

	// Responses
	public static final String RES_OK = "OK";
	public static final String RES_USING = "USING"; // USING <tube>\r\n
	public static final String RES_FOUND = "FOUND"; // FOUND <id> <bytes>\r\n<data>\r\n
	public static final String RES_INSERTED = "INSERTED"; // INSERTED <id>\r\n
	public static final String RES_RESERVED = "RESERVED"; // FOUND <id> <bytes>\r\n<data>\r\n
	public static final String RES_WATCHING = "WATCHING"; // WATCHING <tube>\r\n
	public static final String RES_RELEASED = "RELEASED";
	public static final String RES_TOUCHED = "TOUCHED";
	public static final String RES_DELETED = "DELETED";
	public static final String RES_BURIED = "BURIED"; // BURIED\r\n | BURIED <id>\r\n
	public static final String RES_KICKED = "KICKED"; // KICKED\r\n | KICKED <count>\r\n
	public static final String RES_PAUSED = "PAUSED";

}
