package org.opencloudb.route.handler;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.ServerRouterUtil;

import java.sql.SQLNonTransientException;


/**
 * 处理注释中类型为schema 的情况（按照指定schema做路由解析）
 */
public class HintSchemaHandler implements HintHandler {

    private static final Logger LOGGER = Logger
            .getLogger(HintSchemaHandler.class);

    /**
     * 从全局的schema列表中查询指定的schema是否存在，
     *    如果存在则替换connection属性中原有的schema，
     *    如果不存在，则throws SQLNonTransientException，表示指定的schema 不存在
     * @param sysConfig
     * @param schema
     * @param sqlType
     * @param realSQL
     * @param charset
     * @param info
     * @param cachePool
     * @param hintSQLValue
     * @return
     * @throws SQLNonTransientException
     */
    @Override
    public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
                                String realSQL, String charset, Object info, LayerCachePool cachePool,
                                String hintSQLValue) throws SQLNonTransientException {
        schema = MycatServer.getInstance().getConfig()
                .getSchemas().get(hintSQLValue);
        if(schema != null){
            RouteResultset rrs = ServerRouterUtil.route(sysConfig, schema, sqlType, realSQL,
                    charset, info, cachePool);
            return rrs;
        }else{
            String msg = "can't find schema:" + schema.getName();
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
    }
}
