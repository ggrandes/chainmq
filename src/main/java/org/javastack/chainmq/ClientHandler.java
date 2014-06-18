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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Client Handler
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class ClientHandler {
	private static final Logger log = Logger.getLogger(ClientHandler.class);
	private static final Charset charsetLatin1 = Charset.forName("ISO-8859-1");
	// Connection
	final ByteBuffer bufIn = ByteBuffer.allocate(4096);
	final ByteBuffer bufOut = ByteBuffer.allocate(4096);
	final Server srv;
	final Selector s;
	final SocketChannel sc;
	final ClientContext cc;
	// State
	boolean pausedRead = false;
	boolean doClose = false;
	int protoState = Constants.PROTO_READ_LINE;
	int needData = 0;
	// Tokens
	List<String> tokens;

	public ClientHandler(final Server srv, final Selector s, final SocketChannel sc, final AsyncTaskTracker tracker) {
		this.srv = srv;
		this.s = s;
		this.sc = sc;
		this.cc = new ClientContext(srv.getReservedJobsExpirer(), this, tracker);
		tokensClear();
	}

	public String getName() {
		return Integer.toHexString(hashCode() | 0x80000000);
	}
	
	/**
	 * Read pending data from client
	 * 
	 * @return
	 * @throws IOException
	 */
	public int doRead() throws IOException {
		if (doClose)
			doClose();
		int len = sc.read(bufIn);
		if (len < 0) {
			sc.close();
			throw new ClosedChannelException();
		}
		if ((len == 0) && (!bufIn.hasRemaining())) { // Overflow
			// Overflow
			queueClose();
			log.warn("Input overflow: " + bufIn);
			responseMessage(Constants.ERROR_BAD_FORMAT);
			return len;
		}
		while (!pausedRead) {
			// Read Line
			if (protoState == Constants.PROTO_READ_LINE)
				doReadLine();
			// Read Data + End-CRLF
			if (protoState == Constants.PROTO_READ_BODY) {
				if (doReadBody() && (bufIn.position() > 0))
					continue;
			}
			break;
		}
		return len;
	}

	// Reset state to Read Line
	void reset() {
		needData = 0;
		protoState = Constants.PROTO_READ_LINE;
		tokensClear();
	}

	void doReadLine() throws IOException {
		int hipos = bufIn.position();
		int pos = 0;
		while (pos < hipos && !pausedRead) {
			bufIn.position(pos);
			pos++;
			final byte b = bufIn.get();
			if ((b == '\r') && (pos++ < hipos) && (bufIn.get() == '\n')) {
				final String msg = new String(bufIn.array(), 0, pos - 2, charsetLatin1);
				tokens = Utils.parseTokens(msg);
				if (log.isDebugEnabled())
					log.debug(getName() + " New message: <" + dumpBuffer(msg.getBytes(charsetLatin1)) + "> tokens=" + tokensCount() + " valid=" + tokensAreValid());
				// Compact
				bufIn.limit(hipos).position(pos);
				bufIn.compact();
				//
				try {
					if (!tokensAreValid())
						throw new HandlerException(Constants.ERROR_BAD_FORMAT);

					if (tokens.get(0).isEmpty())
						throw new HandlerException(Constants.ERROR_UNKNOWN_COMMAND);

					final CommandHandler handler = CommandHandler.getHandler(tokens.get(0));
					if (handler == null)
						throw new HandlerException(Constants.ERROR_UNKNOWN_COMMAND);

					final int expectedTokens = handler.expectedTokens();
					if ((expectedTokens > 0) && (expectedTokens != tokensCount()))
						throw new HandlerException(Constants.ERROR_BAD_FORMAT);

					final boolean expectedData = handler.expectedData();
					if (expectedData) {
						needData = handler.getDataLength(this, tokens);
						protoState = Constants.PROTO_READ_BODY;
						if (log.isDebugEnabled())
							log.debug("Reading bodyData length expected: " + needData);
						break;
					} else {
						needData = 0;
						pauseRead();
						if (log.isDebugEnabled())
							log.debug(getName() + " REQUEST: " + tokens);
						handler.handle(this, tokens);
					}
				} catch (HandlerException e) {
					responseMessage(e.getMessage());
				} catch (OutOfMemoryError e) {
					log.error("OutOfMemoryError: " + e.toString(), e);
					responseMessage(Constants.ERROR_OUT_OF_MEMORY);
				} catch (Exception e) {
					queueClose();
					log.error("Exception: " + e.toString(), e);
					responseMessage(Constants.ERROR_INTERNAL_ERROR);
				} finally {
					pos = 0;
					hipos = bufIn.position();
				}
				if (!sc.isOpen())
					break;
				//
				tokensClear();
			}
		}
	}

	boolean doReadBody() throws IOException {
		int hipos = bufIn.position();
		if (hipos < (needData + 2)) { // data + CRLF
			return false;
		}
		try {
			bufIn.flip();
			byte[] buf = new byte[needData];
			bufIn.get(buf);
			if ((bufIn.get() == '\r') && (bufIn.get() == '\n')) {
				pauseRead();
				final CommandHandler handler = CommandHandler.getHandler(tokens.get(0));
				if (log.isDebugEnabled())
					log.debug(getName() + " REQUEST: " + tokens);
				handler.handle(this, tokens, buf);
			} else {
				throw new HandlerException(Constants.ERROR_EXPECTED_CRLF);
			}
		} catch (HandlerException e) {
			responseMessage(e.getMessage());
		} catch (OutOfMemoryError e) {
			log.error("OutOfMemoryError: " + e.toString(), e);
			responseMessage(Constants.ERROR_OUT_OF_MEMORY);
		} catch (Exception e) {
			queueClose();
			log.error("Exception: " + e.toString(), e);
			responseMessage(Constants.ERROR_INTERNAL_ERROR);
		}
		// Compact
		bufIn.limit(hipos).position(needData + 2);
		bufIn.compact();
		hipos = bufIn.position();
		//
		reset();
		//
		return true;
	}

	/**
	 * Signal connection for close
	 */
	public void queueClose() {
		doClose = true;
	}

	/**
	 * Close connection
	 * 
	 * @throws IOException
	 */
	public void doClose() {
		log.info(getName() + " End connection");
		try {
			sc.close();
		} catch (IOException e) {
		}
		doClean();
	}

	public void doClean() {
		log.info(getName() + " DoClean");
		cc.freeResources();
	}


	/**
	 * Write pending output buffer
	 * 
	 * @return
	 * @throws IOException
	 */
	public int doWrite() throws IOException {
		if (log.isDebugEnabled())
			log.debug("DoWrite()");
		bufOut.flip();
		int len = sc.write(bufOut);
		bufOut.clear();
		if (doClose) {
			doClose();
		} else {
			if (log.isDebugEnabled())
				log.debug(getName() + " Marked for READ");
			sc.register(s, SelectionKey.OP_READ, this);
			pausedRead = false;
			doRead(); // Process pending buffers
		}
		return len;
	}

	/**
	 * Pause read of data from client
	 * 
	 * @throws ClosedChannelException
	 */
	public void pauseRead() throws ClosedChannelException {
		if (log.isDebugEnabled())
			log.debug(getName() + " Paused READ");
		final SelectionKey sk = sc.keyFor(s);
		sc.register(s, (sk.interestOps() & ~SelectionKey.OP_READ), this);
		pausedRead = true;
	}

	private void tokensClear() {
		if (tokens != null)
			tokens = null;
	}

	private int tokensCount() {
		if (tokens == null)
			return 0;
		return tokens.size();
	}

	private boolean tokensAreValid() {
		if (tokens == null)
			return false;
		return (tokens.size() <= Constants.REQUEST_MAX_TOKENS);
	}

	public ClientContext getContext() {
		return cc;
	}

	public Server getServer() {
		return srv;
	}

	public void responseMessage(final String msg) throws ClosedChannelException {
		// MESSAGE\r\n
		if (log.isDebugEnabled())
			log.debug(getName() + " RESPONSE: " + msg);
		bufOut.put(msg.getBytes(charsetLatin1)).put((byte) '\r').put((byte) '\n');
		sc.register(s, SelectionKey.OP_WRITE, this);
		s.wakeup();
	}

	public void responseMessage(final String msg, final long p1) throws ClosedChannelException {
		// MESSAGE <number>\r\n
		if (log.isDebugEnabled())
		log.debug(getName() + " RESPONSE: " + msg + " " + p1);
		bufOut.put(msg.getBytes(charsetLatin1)).put((byte) ' ')
				.put(Long.toString(p1).getBytes(charsetLatin1)).put((byte) '\r').put((byte) '\n');
		sc.register(s, SelectionKey.OP_WRITE, this);
		s.wakeup();
	}

	public void responseMessage(final String msg, final String p1) throws ClosedChannelException {
		// MESSAGE <text>\r\n
		if (log.isDebugEnabled())
			log.debug(getName() + " RESPONSE: " + msg + " " + p1);
		bufOut.put(msg.getBytes(charsetLatin1)).put((byte) ' ').put(p1.getBytes(charsetLatin1))
				.put((byte) '\r').put((byte) '\n');
		sc.register(s, SelectionKey.OP_WRITE, this);
		s.wakeup();
	}

	public void responseMessage(final String msg, final int datalen, final byte[] data)
			throws ClosedChannelException {
		// MESSAGE <bytes>\r\n<data>\r\n
		if (log.isDebugEnabled())
			log.debug(getName() + " RESPONSE: " + msg + " " + datalen);
		bufOut.put(msg.getBytes(charsetLatin1)).put((byte) ' ')
				.put(Integer.toString(datalen).getBytes(charsetLatin1)).put((byte) '\r').put((byte) '\n')
				.put(data, 0, datalen).put((byte) '\r').put((byte) '\n');
		sc.register(s, SelectionKey.OP_WRITE, this);
		s.wakeup();
	}

	public void responseMessage(final String msg, final long p1, final int datalen, final byte[] data)
			throws ClosedChannelException {
		// MESSAGE <number> <datalen>\r\n<data>\r\n
		if (log.isDebugEnabled())
		log.debug(getName() + " RESPONSE: " + msg + " " + p1 + " " + datalen);
		bufOut.put(msg.getBytes(charsetLatin1)).put((byte) ' ')
				.put(Long.toString(p1).getBytes(charsetLatin1)).put((byte) ' ')
				.put(Integer.toString(datalen).getBytes(charsetLatin1)).put((byte) '\r').put((byte) '\n')
				.put(data, 0, datalen).put((byte) '\r').put((byte) '\n');
		sc.register(s, SelectionKey.OP_WRITE, this);
		s.wakeup();
	}

	private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

	public static String dumpBuffer(final byte[] input) {
		final StringBuilder sb = new StringBuilder(input.length << 1);
		for (int j = 0; j < input.length; j++) {
			final byte b = input[j];
			if ((b >= 'A') && (b <= 'Z'))
				sb.append((char) b);
			else if ((b >= 'a') && (b <= 'z'))
				sb.append((char) b);
			else if ((b >= '0') && (b <= '9'))
				sb.append((char) b);
			else if ("$()+-./;_ ".indexOf(b) != -1)
				sb.append((char) b);
			else {
				sb.append("<").append(HEX_CHARS[((int) (b >>> (1 << 2))) & 0xF])
						.append(HEX_CHARS[((int) (b >>> (0 << 2))) & 0xF]).append(">");
			}
		}
		return sb.toString();
	}

}
