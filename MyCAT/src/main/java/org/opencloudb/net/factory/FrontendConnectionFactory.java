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
package org.opencloudb.net.factory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;

import org.opencloudb.buffer.BufferQueue;
import org.opencloudb.net.FrontendConnection;

/**
 * @author mycat
 */
public abstract class FrontendConnectionFactory {

	protected int packetHeaderSize = 4;
	protected int maxPacketSize = 16 * 1024 * 1024;
	protected int writeQueueCapcity = 16;
	protected long idleTimeout = 8 * 3600 * 1000L;
	protected String charset = "utf8";

	protected abstract FrontendConnection getConnection(
			AsynchronousSocketChannel channel) throws IOException;

	public FrontendConnection make(AsynchronousSocketChannel channel)
			throws IOException {
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

		FrontendConnection c = getConnection(channel);
		c.setPacketHeaderSize(packetHeaderSize);
		c.setMaxPacketSize(maxPacketSize);
		c.setIdleTimeout(idleTimeout);
		c.setCharset(charset);
		return c;
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

	public int getWriteQueueCapcity() {
		return writeQueueCapcity;
	}

	public void setWriteQueueCapcity(int writeQueueCapcity) {
		this.writeQueueCapcity = writeQueueCapcity;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

}