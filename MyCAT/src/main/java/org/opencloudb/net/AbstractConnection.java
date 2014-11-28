/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection {
	protected static final Logger LOGGER = Logger
			.getLogger(AbstractConnection.class);
	protected final AsynchronousSocketChannel channel;
	protected NIOProcessor processor;
	protected NIOHandler handler;
	protected SelectionKey processKey;
	protected int packetHeaderSize;
	protected int maxPacketSize;
	protected volatile int readBufferOffset;
	private volatile ByteBuffer readBuffer;
	private volatile ByteBuffer writeBuffer;
	private volatile boolean writing = false;
	// private volatile boolean writing = false;
	protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	protected boolean isRegistered;
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;
	private final ReentrantLock asynWriteCheckLock = new ReentrantLock();
	private long idleTimeout;
	private AIOReadHandler aioReadHandler = new AIOReadHandler();
	private AIOWriteHandler aioWriteHandler = new AIOWriteHandler();
	private final WriteEventCheckRunner writeEventChkRunner = new WriteEventCheckRunner();

	public AbstractConnection(AsynchronousSocketChannel channel) {
		this.channel = channel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
				lastReadTime) + idleTimeout;

	}

	public AsynchronousSocketChannel getChannel() {
		return channel;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public void setProcessor(NIOProcessor processor) {
		this.processor = processor;
		this.readBuffer = processor.getBufferPool().allocate();
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public int getWriteAttempts() {
		return writeAttempts;
	}

	public NIOProcessor getProcessor() {
		return processor;
	}

	public ByteBuffer getReadBuffer() {
		return readBuffer;
	}

	public ByteBuffer allocate() {
		ByteBuffer buffer = this.processor.getBufferPool().allocate();
		return buffer;
	}

	public final void recycle(ByteBuffer buffer) {
		this.processor.getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler handler) {
		this.handler = handler;
	}

	@Override
	public void handle(byte[] data) {
		try {
			handler.handle(data);
		} catch (Throwable e) {
			close("exeption:" + e.toString());
			if (e instanceof ConnectionException) {
				error(ErrorCode.ERR_CONNECT_SOCKET, e);
			} else {
				error(ErrorCode.ERR_HANDLE_DATA, e);
			}
		}
	}

	@Override
	public void register() throws IOException {

	}

	public void asynRead() {

		ByteBuffer theBuffer = readBuffer;
		if (theBuffer == null) {
			theBuffer = processor.getBufferPool().allocate();
			this.readBuffer = theBuffer;
			channel.read(theBuffer, this, aioReadHandler);

		} else if (theBuffer.hasRemaining()) {
			channel.read(theBuffer, this, aioReadHandler);
		} else {
			throw new java.lang.IllegalArgumentException("full buffer to read ");
		}
	}

	public void onReadData(int got) throws IOException {
		if (isClosed.get()) {
			return;
		}
		ByteBuffer buffer = this.readBuffer;
		lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			if (!this.isClosed()) {
				this.close("socket closed");
				return;
			}
		} else if (got == 0) {
			return;
		}
		netInBytes += got;
		processor.addNetInBytes(got);

		// 澶勭悊鏁版嵁
		int offset = readBufferOffset, length = 0, position = buffer.position();
		for (;;) {
			length = getPacketLength(buffer, offset);
			if (length == -1) {
				if (!buffer.hasRemaining()) {
					buffer = checkReadBuffer(buffer, offset, position);
				}
				break;
			}
			if (position >= offset + length) {
				buffer.position(offset);
				byte[] data = new byte[length];
				buffer.get(data, 0, length);
				handle(data);

				offset += length;
				if (position == offset) {
					if (readBufferOffset != 0) {
						readBufferOffset = 0;
					}
					buffer.clear();
					break;
				} else {
					readBufferOffset = offset;
					buffer.position(position);
					continue;
				}
			} else {
				if (!buffer.hasRemaining()) {
					buffer = checkReadBuffer(buffer, offset, position);
				}
				break;
			}
		}
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);
	}

	private final void writeNotSend(ByteBuffer buffer) {
		writeQueue.offer(buffer);
	}

	@Override
	public final void write(ByteBuffer buffer) {
		writeQueue.offer(buffer);
		// if ansyn write finishe event got lock before me ,then writing
		// flag is set false but not start a write request
		// so we check again
		if (writing == false) {
			doNextWriteCheck();
		}

	}

	private void doNextWriteCheck() {
		if (writing == false) {
			if (!asynWriteCheckLock.isLocked()) {
				try {
					asynWriteCheckLock.lock();
					if (writing) {
						return;
					}
					ByteBuffer buffer = writeQueue.poll();
					if (buffer != null) {
						if (buffer.limit() == 0) {
							this.recycle(buffer);
							this.writeBuffer = null;
							this.close("quit cmd");
						} else {
							writing = true;
							writeBuffer = buffer;
							asynWrite(buffer);
						}
					}
				} finally {
					asynWriteCheckLock.unlock();
				}
			} else {// next run a thread asyn check again
				processor.getExecutor().execute(writeEventChkRunner);
			}
		}
	}

	private void asynWrite(ByteBuffer buffer) {
		buffer.flip();
		this.channel.write(buffer, this, aioWriteHandler);
	}

	public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity,
			boolean writeSocketIfFull) {
		if (capacity > buffer.remaining()) {
			if (writeSocketIfFull) {
				writeNotSend(buffer);
				return allocate();
			} else {// Relocate a larger buffer
				buffer.flip();
				ByteBuffer newBuf = ByteBuffer.allocate(capacity);
				newBuf.put(buffer);
				this.recycle(buffer);
				return newBuf;
			}
		} else {
			return buffer;
		}
	}

	public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;
		int remaining = buffer.remaining();
		while (length > 0) {
			if (remaining >= length) {
				buffer.put(src, offset, length);
				break;
			} else {
				buffer.put(src, offset, remaining);
				writeNotSend(buffer);
				buffer = allocate();
				offset += remaining;
				length -= remaining;
				remaining = buffer.remaining();
				continue;
			}
		}
		return buffer;
	}

	@Override
	public void close(String reason) {
		if (!isClosed.get()) {
			closeSocket();
			isClosed.set(true);
			this.cleanup();
			LOGGER.info("close connection,reason:" + reason + " " + this);
		}
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.info(toString() + " idle timeout");
			close(" idle ");
		}
	}

	/**
	 * 娓呯悊閬楃暀璧勬簮
	 */
	protected void cleanup() {

		// 鍥炴敹鎺ユ敹缂撳瓨
		if (readBuffer != null) {
			recycle(readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}

		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}

	protected int getPacketLength(ByteBuffer buffer, int offset) {
		if (buffer.position() < offset + packetHeaderSize) {
			return -1;
		} else {
			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
			return length + packetHeaderSize;
		}
	}

	private ByteBuffer checkReadBuffer(ByteBuffer buffer, int offset,
			int position) {
		if (offset == 0) {
			if (buffer.capacity() >= maxPacketSize) {
				throw new IllegalArgumentException(
						"Packet size over the limit.");
			}
			int size = buffer.capacity() << 1;
			size = (size > maxPacketSize) ? maxPacketSize : size;
			ByteBuffer newBuffer = processor.getBufferPool().allocate(size);
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;
			recycle(buffer);
			return newBuffer;
		} else {
			buffer.position(offset);
			buffer.compact();
			readBufferOffset = 0;
			return buffer;
		}
	}

	protected void onWriteFinished(int result) {
		netOutBytes += result;
		processor.addNetOutBytes(result);
		lastWriteTime = TimeUtil.currentTimeMillis();

		ByteBuffer theBuffer = writeBuffer;
		if (theBuffer == null || !theBuffer.hasRemaining()) {// writeFinished,但要区分bufer是否NULL，不NULL，要回收
			if (theBuffer != null) {
				this.recycle(theBuffer);
				writeBuffer = null;
			}
			this.writing = false;

			this.doNextWriteCheck();
		} else {
			theBuffer.compact();
			asynWrite(theBuffer);
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	private void closeSocket() {

		if (channel != null) {
			boolean isSocketClosed = true;
			try {
				channel.close();
			} catch (Throwable e) {
			}
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}

		}
	}

	class WriteEventCheckRunner implements Runnable {
		private volatile boolean running = false;

		@Override
		public void run() {
			doNextWriteCheck();
		}

		public boolean isRunning() {
			return running;
		}
	}

}

class AIOWriteHandler implements CompletionHandler<Integer, AbstractConnection> {

	@Override
	public void completed(final Integer result, final AbstractConnection con) {
		try {
			if (result >= 0) {
				con.onWriteFinished(result);
			} else {
				con.close("write erro " + result);
			}
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("caught aio process err:", e);
		}

	}

	@Override
	public void failed(Throwable exc, AbstractConnection con) {
		con.close("write failed " + exc);
	}

}

class AIOReadHandler implements CompletionHandler<Integer, AbstractConnection> {
	@Override
	public void completed(final Integer i, final AbstractConnection con) {
		// con.getProcessor().getExecutor().execute(new Runnable() {
		// public void run() {
		if (i > 0) {
			try {
				con.onReadData(i);
				con.asynRead();
			} catch (IOException e) {
				con.close("handle err:" + e);
			}
		} else if (i == -1) {
			// System.out.println("read -1 xxxxxxxxx "+con);
			con.close("client closed");
		}
		// }
		// });
	}

	@Override
	public void failed(Throwable exc, AbstractConnection con) {
		con.close(exc.toString());

	}
}
