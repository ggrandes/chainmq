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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Command Handler
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public abstract class CommandHandler {
	private static final HashMap<String, CommandHandler> commandHandlers = new HashMap<String, CommandHandler>();

	static {
		registerCommand("put", new PutCommand());
		registerCommand("use", new UseCommand());
		registerCommand("reserve", new ReserveCommand());
		registerCommand("reserve-with-timeout", new ReserveWithTimeoutCommand());
		registerCommand("delete", new DeleteCommand());
		registerCommand("release", new ReleaseCommand());
		registerCommand("bury", new BuryCommand());
		registerCommand("touch", new TouchCommand());
		registerCommand("watch", new WatchCommand());
		registerCommand("ignore", new IgnoreCommand());
		registerCommand("peek", new PeekCommand());
		registerCommand("peek-ready", new PeekReadyCommand());
		registerCommand("peek-delayed", new PeekDelayedCommand());
		registerCommand("peek-buried", new PeekBuriedCommand());
		registerCommand("kick", new KickCommand());
		registerCommand("kick-job", new KickJobCommand());
		registerCommand("stats-job", new StatsJobCommand());
		registerCommand("stats-tube", new StatsTubeCommand());
		registerCommand("stats", new StatsCommand());
		registerCommand("list-tubes", new ListTubesCommand());
		registerCommand("list-tube-used", new ListTubeUsedCommand());
		registerCommand("list-tubes-watched", new ListTubesWatchedCommand());
		registerCommand("quit", new QuitCommand());
		registerCommand("pause-tube", new PauseTubeCommand());
	}

	public static CommandHandler getHandler(final String cmd) {
		return commandHandlers.get(cmd);
	}

	private static void registerCommand(final String cmd, final CommandHandler commandHandler) {
		commandHandlers.put(cmd, commandHandler);
	}

	/**
	 * Check String for valid/invalid name
	 * 
	 * <pre>
	 * Characters Allowed:
	 * - letters (A-Z and a-z)
	 * - numerals (0-9)
	 * - hyphen ("-")
	 * - plus ("+")
	 * - slash ("/")
	 * - semicolon (";")
	 * - dot (".")
	 * - dollar-sign ("$")
	 * - underscore ("_")
	 * - parentheses ("(" and ")")
	 * </pre>
	 * 
	 * @param input
	 * @return true if valid
	 */
	public static final boolean checkValidName(final String input) {
		if (input == null)
			return false;
		final int len = input.length();
		if (len < 1)
			return false;
		if (len > 200)
			return false;
		if (input.charAt(0) == '-')
			return false;
		for (int i = 0; i < len; i++) {
			final char c = input.charAt(i);
			if ((c >= 'A') && (c <= 'Z'))
				;
			else if ((c >= 'a') && (c <= 'z'))
				;
			else if ((c >= '0') && (c <= '9'))
				;
			else {
				switch (c) {
				case '$':
				case '(':
				case ')':
				case '+':
				case '-':
				case '.':
				case '/':
				case ';':
				case '_':
					break;
				default:
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Return the number of tokens expected or -1 if no check must be performed
	 * 
	 * @return
	 */
	public abstract int expectedTokens();

	/**
	 * Return true if payload is expected
	 * 
	 * @return
	 * @see #getDataLength(ClientHandler, List)
	 */
	public boolean expectedData() {
		return false;
	}

	/**
	 * Return length of payload
	 * 
	 * @param clientHandler
	 * @param tokens
	 * @return
	 * @throws HandlerException
	 *             , ClosedChannelException
	 * @see #expectedData()
	 */
	public int getDataLength(final ClientHandler clientHandler, final List<String> tokens)
			throws HandlerException, ClosedChannelException {
		return -1;
	}

	/**
	 * Handle the command
	 * 
	 * @param clientHandler
	 * @param tokens
	 *            (command, arg1, arg2,...)
	 * @throws HandlerException
	 *             , IOException
	 */
	public void handle(final ClientHandler clientHandler, final List<String> tokens) throws HandlerException,
			IOException {
		throw new HandlerException(Constants.ERROR_INTERNAL_ERROR);
	}

	/**
	 * Handle the command with data
	 * 
	 * @param clientHandler
	 * @param tokens
	 *            (command, arg1, arg2,...)
	 * @param buf
	 * @throws HandlerException
	 *             , IOException
	 */
	public void handle(final ClientHandler clientHandler, final List<String> tokens, final byte[] buf)
			throws HandlerException, IOException {
		throw new HandlerException(Constants.ERROR_INTERNAL_ERROR);
	}

	// ----------------- COMMANDS -----------------

	// put <pri> <delay> <ttr> <bytes>\r\n
	// <data>\r\n
	static class PutCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 5;
		}

		@Override
		public boolean expectedData() {
			return true;
		}

		@Override
		public int getDataLength(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, ClosedChannelException {
			final int len = Utils.parseInteger(tokens.get(4));
			if (len < 0) {
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			}
			return len;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens, final byte[] data)
				throws HandlerException, IOException {
			final String tubeName = clientHandler.getContext().getCurrentTube();
			Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final long prio = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			final long delay = Utils.parseLong(tokens.get(2), Constants.MAX_INT_32BITS);
			final long ttr = Math.max(1, Utils.parseLong(tokens.get(3), Constants.MAX_INT_32BITS));
			if ((prio < 0) || (delay < 0) || (ttr < 0))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			// INSERTED <id>\r\n
			final Job job = tube.newJob(prio, delay, ttr, data);
			job.doNew();
			clientHandler.responseMessage(Constants.RES_INSERTED, job.id);
		}
	}

	// use <tube>\r\n
	static class UseCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final String tubeName = tokens.get(1);
			if (!checkValidName(tubeName))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			clientHandler.getContext().setCurrentTube(tubeName);
			TubeMapper.getInstance().getTubeOrCreate(tubeName);
			clientHandler.responseMessage(Constants.RES_USING, tubeName);
		}
	}

	// reserve\r\n
	static class ReserveCommand extends ReserveWithTimeoutCommand {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			reserveWithTimeout(clientHandler, -1);
		}
	}

	// reserve-with-timeout <seconds>\r\n
	static class ReserveWithTimeoutCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long seconds = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (seconds < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			reserveWithTimeout(clientHandler, seconds);
		}
		
		void reserveWithTimeout(final ClientHandler clientHandler, final long seconds)
				throws HandlerException, IOException {
			final ClientContext context = clientHandler.getContext();
			final AsyncTaskTracker tracker = context.getTracker();
			final long seqReq = tracker.newTaskId();
			
			// Handle DEADLINE_SOON / TIMED_OUT
			long reserveTaskCancelatorDelay = 0;
			String reserveTaskCancelatorMsg = null;
			//
			final Job nearExpire = context.jobReservedNearExpire();
			final long deadLineSoon = ((nearExpire == null) ? 0 : Math.max(1, (nearExpire.reserveExpire - Constants.SAFETY_MARGIN - System.currentTimeMillis())));
			final long timedOut = (Math.max(0, seconds) * 1000);
			if ((deadLineSoon > 0) && (deadLineSoon <= timedOut)) {
				reserveTaskCancelatorMsg = Constants.ERROR_DEADLINE_SOON;
				reserveTaskCancelatorDelay = deadLineSoon;
			} else {
				reserveTaskCancelatorMsg = Constants.ERROR_TIMED_OUT;
				reserveTaskCancelatorDelay = timedOut;
			}
			//
			if (reserveTaskCancelatorDelay > 0) {
				final String msg = reserveTaskCancelatorMsg;
				final AsyncTaskRunner reserveTaskCancelator = new AsyncTaskRunner(seqReq, clientHandler) {
					@Override
					public void processAsyncRequest() {
						try {
							if (!taskMarkDone())
								return;
							clientHandler.responseMessage(msg);
						} catch (ClosedChannelException e) {
							e.printStackTrace(System.out);
						}
					}
				};
				tracker.submitAsyncTask(reserveTaskCancelator, reserveTaskCancelatorDelay);
			}
			
			// Callback for: NOT_FOUND / RESERVED 
			final Tube.AsyncUpdateCallBack cbClient = new Tube.AsyncUpdateCallBack(seqReq, clientHandler) {
				@Override
				public boolean newData(final Tube tube) {
					if (taskIsDone())
						return false;
					final Job job = tube.get();
					if (job == null) {
						if (seconds == 0) {
							if (!taskMarkDone())
								return false;
							try {
								clientHandler.responseMessage(Constants.ERROR_NOT_FOUND);
							} catch (ClosedChannelException e) {
								e.printStackTrace(System.out);
							}
							return true;
						}
						// register callback
						tube.pushCallback(this);
						return true;
					}
					context.jobReserve(job);
					if (!taskMarkDone()) {
						// Ignore
						Logger.getLogger(getClass()).warn("Ignored already ACKed seq=" + sequence);
						return false;
					}

					try {
						// RESERVED <id> <bytes>\r\n<data>\r\n
						clientHandler.responseMessage(Constants.RES_RESERVED, job.id, job.body.length, job.body);
					} catch (ClosedChannelException e) {
						e.printStackTrace(System.out);
					}
					return true;
				}
			};
			final Set<String> tubes = context.getWatchedTubes();
			for (final String tubeName : tubes) {
				final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
				cbClient.newData(tube);
			}
		}
	}

	// delete <id>\r\n
	static class DeleteCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (id < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Job job = TubeMapper.getInstance().getJob(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			final ClientContext context = clientHandler.getContext();
			if (job.isReserved() && !job.isReserved(context))
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// DELETED
			job.setDeleted();
			clientHandler.responseMessage(Constants.RES_DELETED);
		}
	}

	// release <id> <pri> <delay>\r\n
	static class ReleaseCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 4;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			final long prio = Utils.parseLong(tokens.get(2), Constants.MAX_INT_32BITS);
			final long delay = Utils.parseLong(tokens.get(3), Constants.MAX_INT_32BITS);
			if ((id < 0) || (prio < 0) || (delay < 0))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Job job = TubeMapper.getInstance().getJob(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			final ClientContext context = clientHandler.getContext();
			if (!job.isReserved(context))
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// RELEASED
			job.doRelease(prio, delay);
			clientHandler.responseMessage(Constants.RES_RELEASED);
		}
	}

	// bury <id> <pri>\r\n
	static class BuryCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 3;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			final long prio = Utils.parseLong(tokens.get(2), Constants.MAX_INT_32BITS);
			if ((id < 0) || (prio < 0))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Job job = TubeMapper.getInstance().getJob(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			final ClientContext context = clientHandler.getContext();
			if (!job.isReserved(context))
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// BURIED
			job.setBuried(prio);
			clientHandler.responseMessage(Constants.RES_BURIED);
		}
	}

	// touch <id>\r\n
	static class TouchCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (id < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Job job = TubeMapper.getInstance().getJob(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			final ClientContext context = clientHandler.getContext();
			if (!job.isReserved(context))
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// TOUCHED
			context.jobReserve(job);
			clientHandler.responseMessage(Constants.RES_TOUCHED);
		}
	}

	// watch <tube>\r\n
	static class WatchCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final String tubeName = tokens.get(1);
			if (!checkValidName(tubeName))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			// WATCHING
			ClientContext context = clientHandler.getContext();
			context.addWatchedTube(tubeName);
			TubeMapper.getInstance().getTubeOrCreate(tubeName);
			clientHandler.responseMessage(Constants.RES_WATCHING, context.getWatchedTubes().size());
		}
	}

	// ignore <tube>\r\n
	static class IgnoreCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final String tubeName = tokens.get(1);
			if (!checkValidName(tubeName))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			// WATCHING
			ClientContext context = clientHandler.getContext();
			context.ignoreWatchedTube(tubeName);
			clientHandler.responseMessage(Constants.RES_WATCHING, context.getWatchedTubes().size());
		}
	}

	// peek <id>\r\n
	static class PeekCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (id < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final String tubeName = clientHandler.getContext().getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final Job job = tube.peek(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// FOUND <id> <bytes>\r\n<data>\r\n
			clientHandler.responseMessage(Constants.RES_FOUND, job.id, job.body.length, job.body);
		}
	}

	// peek-ready\r\n
	static class PeekReadyCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final ClientContext context = clientHandler.getContext();
			final String tubeName = context.getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final Job job = tube.peek();
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// FOUND <id> <bytes>\r\n<data>\r\n
			clientHandler.responseMessage(Constants.RES_FOUND, job.id, job.body.length, job.body);
		}
	}

	// peek-delayed\r\n
	static class PeekDelayedCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final ClientContext context = clientHandler.getContext();
			final String tubeName = context.getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final Job job = tube.peekDelayed();
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// FOUND <id> <bytes>\r\n<data>\r\n
			clientHandler.responseMessage(Constants.RES_FOUND, job.id, job.body.length, job.body);
		}
	}

	// peek-buried\r\n
	static class PeekBuriedCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final ClientContext context = clientHandler.getContext();
			final String tubeName = context.getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final Job job = tube.peekBuried();
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// FOUND <id> <bytes>\r\n<data>\r\n
			clientHandler.responseMessage(Constants.RES_FOUND, job.id, job.body.length, job.body);
		}
	}

	// kick <bound>\r\n
	static class KickCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long bound = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (bound < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final ClientContext context = clientHandler.getContext();
			final String tubeName = context.getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final long count = tube.kick(bound);
			// KICKED <count>\r\n
			clientHandler.responseMessage(Constants.RES_KICKED, count);
		}
	}

	// kick-job <id>\r\n
	static class KickJobCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (id < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Job job = TubeMapper.getInstance().getJob(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// DELETED
			if (!job.doKick())
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// KICKED
			clientHandler.responseMessage(Constants.RES_KICKED);
		}
	}

	// stats-job <id>\r\n
	static class StatsJobCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final long id = Utils.parseLong(tokens.get(1), Constants.MAX_INT_32BITS);
			if (id < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			//
			final String tubeName = clientHandler.getContext().getCurrentTube();
			final Tube tube = TubeMapper.getInstance().getTubeOrCreate(tubeName);
			final Job job = tube.peek(id);
			// NOT_FOUND\r\n
			if (job == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// OK <bytes>\r\n<data>\r\n
			final String res = SimpleYAML.emitter(job.getStats());
			final byte[] buf = res.getBytes("UTF-8");
			clientHandler.responseMessage(Constants.RES_OK, buf.length, buf);
		}
	}

	// stats-tube <tube>\r\n
	static class StatsTubeCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 2;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final String tubeName = tokens.get(1);
			if (!checkValidName(tubeName))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final Tube tube = TubeMapper.getInstance().getTubeIfExist(tubeName);
			if (tube == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			// OK <bytes>\r\n<data>\r\n
			final String res = SimpleYAML.emitter(tube.getStats());
			final byte[] buf = res.getBytes("UTF-8");
			clientHandler.responseMessage(Constants.RES_OK, buf.length, buf);
		}
	}

	// stats\r\n
	static class StatsCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			// TODO
			final Map<String, Object> map = new LinkedHashMap<String, Object>() {
				private static final long serialVersionUID = 42L;
				{
					put("pid", Integer.toString(Utils.getPid()));
					put("uptime", Integer.toString(clientHandler.getServer().getUptime()));
					put("hostname", Utils.getHostname());
				}
			};
			final String res = SimpleYAML.emitter(map);
			final byte[] buf = res.getBytes("UTF-8");
			clientHandler.responseMessage(Constants.RES_OK, buf.length, buf);
		}
	}

	// list-tubes\r\n
	static class ListTubesCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			// OK <bytes>\r\n<data>\r\n
			final List<String> tubes = TubeMapper.getInstance().getTubeList();
			final String list = SimpleYAML.emitter(tubes);
			final byte[] buf = list.getBytes("UTF-8");
			clientHandler.responseMessage(Constants.RES_OK, buf.length, buf);
		}
	}

	// list-tube-used\r\n
	static class ListTubeUsedCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			// USING
			clientHandler.responseMessage(Constants.RES_USING, clientHandler.getContext().getCurrentTube());
		}
	}

	// list-tubes-watched\r\n
	static class ListTubesWatchedCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			// OK <bytes>\r\n<data>\r\n
			final ClientContext context = clientHandler.getContext();
			final Set<String> tubes = context.getWatchedTubes();
			final String list = SimpleYAML.emitter(tubes);
			final byte[] buf = list.getBytes("UTF-8");
			clientHandler.responseMessage(Constants.RES_OK, buf.length, buf);
		}
	}

	// quit\r\n
	static class QuitCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return -1;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			clientHandler.doClose();
		}
	}

	// pause-tube <tube-name> <delay>\r\n
	static class PauseTubeCommand extends CommandHandler {
		@Override
		public int expectedTokens() {
			return 3;
		}

		@Override
		public void handle(final ClientHandler clientHandler, final List<String> tokens)
				throws HandlerException, IOException {
			final String tubeName = tokens.get(1);
			if (!checkValidName(tubeName))
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			final long delay = Utils.parseLong(tokens.get(2), Constants.MAX_INT_32BITS);
			if (delay < 0)
				throw new HandlerException(Constants.ERROR_BAD_FORMAT);
			// PAUSED
			final Tube tube = TubeMapper.getInstance().getTubeIfExist(tubeName);
			if (tube == null)
				throw new HandlerException(Constants.ERROR_NOT_FOUND);
			tube.pause(delay * 1000);
			clientHandler.responseMessage(Constants.RES_PAUSED);
		}
	}
}
