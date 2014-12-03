package org.opencloudb.route.handler;

import java.util.HashMap;
import java.util.Map;

public class HintHandlerFactory {
	private static boolean isInit = false;
	 //sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema
    private static Map<String,HintHandler> hintHandlerMap = new HashMap<String,HintHandler>();

    private static void init() {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
    }
    
    public static HintHandler getHintHandler(String hintType) {
    	if(!isInit) {
    		init();
    	}
    	return hintHandlerMap.get(hintType);
    }
    
}
