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
package org.opencloudb.server;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.sql.SQLNonTransientException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.SessionSQLPair;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.response.Heartbeat;
import org.opencloudb.server.response.Ping;
import org.opencloudb.util.StringUtil;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
	private static final Logger LOGGER = Logger
			.getLogger(ServerConnection.class);
	private static final long AUTH_TIMEOUT = 15 * 1000L;

	private volatile int txIsolation;
	private volatile boolean autocommit;
	private volatile boolean txInterrupted;
	private volatile String txInterrputMsg = "";
	private long lastInsertId;
	private NonBlockingSession session;

	private AtomicBoolean hasOkRsp = new AtomicBoolean(false);  //表示在Data返回后还有OK Packet报文的语句
	
	public ServerConnection(AsynchronousSocketChannel channel)
			throws IOException {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
	}

	@Override
	public boolean isIdleTimeout() {
		if (isAuthenticated) {
			return super.isIdleTimeout();
		} else {
			return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
					lastReadTime) + AUTH_TIMEOUT;
		}
	}

	public int getTxIsolation() {
		return txIsolation;
	}

	public void setTxIsolation(int txIsolation) {
		this.txIsolation = txIsolation;
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		this.autocommit = autocommit;
	}

	public long getLastInsertId() {
		return lastInsertId;
	}

	public void setLastInsertId(long lastInsertId) {
		this.lastInsertId = lastInsertId;
	}

	/**
	 * 设置是否需要中断当前事务
	 */
	public void setTxInterrupt(String txInterrputMsg) {
		if (!autocommit && !txInterrupted) {
			txInterrupted = true;
			this.txInterrputMsg = txInterrputMsg;
		}
	}

	public NonBlockingSession getSession2() {
		return session;
	}

	public void setSession2(NonBlockingSession session2) {
		this.session = session2;
	}

	@Override
	public void ping() {
		Ping.response(this);
	}

	@Override
	public void heartbeat(byte[] data) {
		Heartbeat.response(this, data);
	}

	public void execute(String sql, int type) {
		if (this.isClosed()) {
			LOGGER.warn("ignore execute ,server connection is closed " + this);
			return;
		}
		// 状态检查
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback." + txInterrputMsg);
			return;
		}

		// 检查当前使用的DB
		String db = this.schema;
		if (db == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"No MyCAT Database selected");
			return;
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return;
		}
		// 检查是否有全局序列号的，需要异步处理
		// @micmiu 简单模糊判断SQL是否包含sequence
		boolean isNeedSequence = false;

		if (sql.indexOf(" MYCATSEQ_") != -1) {
			SessionSQLPair pair = new SessionSQLPair(session, schema, sql,type);
			MycatServer.getInstance().getSequnceProcessor().addNewSql(pair);
			isNeedSequence = true;
		} 
		
		if ( type == ServerParse.INSERT  && !isNeedSequence ) {
				String tableName = StringUtil.getMiddleString(sql.toUpperCase(), "INTO ", " ");
				TableConfig tableConfig = schema.getTables().get(tableName);
				if ( tableConfig.isAutoIncrement() ) {
					String primaryKey = tableConfig.getPrimaryKey();
					int firstLeftBracketIndex = sql.indexOf("(")+1;
					int firstRightBracketIndex = sql.indexOf(")");
					int lastLeftBracketIndex = sql.lastIndexOf("(")+1;
					String insertFieldsSQL = sql.substring(firstLeftBracketIndex, firstRightBracketIndex);
					String[] insertFields = insertFieldsSQL.split(",");
					
					boolean isPrimaryKeyInFields = false;
					for (String field : insertFields) {
						if (field.toUpperCase().equals(primaryKey)){
							isPrimaryKeyInFields = true;
							break;
						}
					}
					if ( !isPrimaryKeyInFields ) {
						StringBuilder sb = new StringBuilder();
						sb.append(sql.substring(0,firstLeftBracketIndex));
						sb.append(primaryKey);
						sb.append(",");
						sb.append(sql.substring(firstLeftBracketIndex,lastLeftBracketIndex));
						sb.append("next value for MYCATSEQ_");
						sb.append(tableName);
						sb.append(",");
						sb.append(sql.substring(lastLeftBracketIndex, sql.length()));

						SessionSQLPair pair = new SessionSQLPair(session, schema, sb.toString(),type);
						MycatServer.getInstance().getSequnceProcessor().addNewSql(pair);
						isNeedSequence = true;
					}
				} else {
					isNeedSequence = false;
				}
		}
		if ( !isNeedSequence ) {
			routeEndExecuteSQL(sql, type, schema);
		}

	}

	public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, type, sql, this.charset, this);
			
			hasOkRsp.set(rrs.isCallStatement());
		} catch (SQLNonTransientException e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(sql).toString() + " err:"
					+ e.toString());
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return;
		}

		// session执行
		session.execute(rrs, type);
	}

	/**
	 * 提交事务
	 */
	public void commit() {
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback.");
		} else {
			session.commit();
		}
	}

	/**
	 * 回滚事务
	 */
	public void rollback() {
		// 状态检查
		if (txInterrupted) {
			txInterrupted = false;
		}

		// 执行回滚
		session.rollback();
	}

	/**
	 * 撤销执行中的语句
	 * 
	 * @param sponsor
	 *            发起者为null表示是自己
	 */
	public void cancel(final FrontendConnection sponsor) {
		processor.getExecutor().execute(new Runnable() {
			@Override
			public void run() {
				session.cancel(sponsor);
			}
		});
	}

	@Override
	public void close(String reason) {

		super.close(reason);
		this.session.clearResources();
		if (this.isClosed()) {
			processor.getExecutor().execute(new Runnable() {
				@Override
				public void run() {
					session.terminate();
				}
			});
			return;
		}
	}

	
	public AtomicBoolean isHasOkRsp() {
		return hasOkRsp;
	}

	@Override
	public String toString() {
		return "ServerConnection [id=" +id +  ", schema=" +schema +", host="+ host +", user="+ user +",txIsolation=" + txIsolation + ", autocommit="
				+ autocommit + ", schema=" +schema + "]";
	}

}