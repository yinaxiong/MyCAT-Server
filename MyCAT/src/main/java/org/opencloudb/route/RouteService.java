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
package org.opencloudb.route;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.handler.HintHandler;
import org.opencloudb.route.handler.HintSQLHandler;
import org.opencloudb.route.handler.HintSchemaHandler;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.StringUtil;

public class RouteService {
    private static final Logger LOGGER = Logger
            .getLogger(RouteService.class);
	private final CachePool sqlRouteCache;
	private final LayerCachePool tableId2DataNodeCache;

    //sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema
    private static Map<String,HintHandler> hintHandlerMap = new HashMap<String,HintHandler>();

    static {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
    }

	public RouteService(CacheService cachService) {
		sqlRouteCache = cachService.getCachePool("SQLRouteCache");
		tableId2DataNodeCache = (LayerCachePool) cachService
				.getCachePool("TableID2DataNodeCache");
	}

	public LayerCachePool getTableId2DataNodeCache() {
		return tableId2DataNodeCache;
	}

	public RouteResultset route(SystemConfig sysconf, SchemaConfig schema,
			int sqlType, String stmt, String charset, Object info)
			throws SQLNonTransientException {
		RouteResultset rrs = null;
		String cacheKey = null;

		if (sqlType == ServerParse.SELECT) {
			cacheKey = schema.getName() + stmt;
			rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
			if (rrs != null) {
				return rrs;
			}
		}

		// 处理自定义分片注释, 注释格式：/*!mycat: type = value */ sql
		String mycatHint = "/*!mycat:";
		/* !mycat: sql = select name from aa */
        /* !mycat: schema = test */
		if (stmt.startsWith(mycatHint)) {
			int endPos = stmt.indexOf("*/");
			if (endPos > 0) {
				// 用!mycat:内部的语句来做路由分析
				String hint = stmt.substring(mycatHint.length(), endPos)
						.trim();

                String[] hints = hint.split("=");
                if(hints.length == 2){
                    String hintType = hints[0].trim().toLowerCase(Locale.US);
                    String hintValue = hints[1].trim();
                    String realSQL = stmt.substring(endPos + "*/".length()).trim();

                    HintHandler hintHandler = hintHandlerMap.get(hintType);
                    if(hintHandler != null){
                        rrs = hintHandler.route(sysconf,schema,sqlType,realSQL,charset,info,tableId2DataNodeCache,
                                hintValue);
                    }else{
                        LOGGER.warn("TODO , support hint sql type : " + hintType );
                    }
                }
			}
		} else {
			stmt = stmt.trim();
			rrs = ServerRouterUtil.route(sysconf, schema, sqlType, stmt,
					charset, info, tableId2DataNodeCache);
		}

		if (sqlType == ServerParse.SELECT && rrs.isCacheAble()) {
			sqlRouteCache.putIfAbsent(cacheKey, rrs);
		}
		return rrs;
	}

}