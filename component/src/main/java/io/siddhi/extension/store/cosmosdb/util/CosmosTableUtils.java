/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.siddhi.extension.store.cosmosdb.util;

import io.siddhi.extension.store.cosmosdb.CosmosCompiledCondition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class which holds the utility methods which are used by various units in the CosmosDB Event Table implementation.
 */
public class CosmosTableUtils {
    private static final Log log = LogFactory.getLog(CosmosTableUtils.class);

    private CosmosTableUtils() {
        //Prevent Initialization.
    }

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }


    /**
     * Util method used throughout the CosmosDB Event Table implementation which accepts a compiled condition (from
     * compile-time) and uses values from the runtime to populate the given condition.
     *
     * @param compiledCondition     the compiled condition which was built during compile time and now is being provided
     *                              by the Siddhi runtime.
     * @param conditionParameterMap the map which contains the runtime value(s) for the condition.
     * @throws SQLException in the unlikely case where there are errors when setting values to the statement
     *                      (e.g. type mismatches)
     */
    public static String resolveCondition(CosmosCompiledCondition compiledCondition,
                                          Map<String, Object> conditionParameterMap) throws SQLException {

        String condition = compiledCondition.getCompiledQuery();
        if (log.isDebugEnabled()) {
            log.debug("compiled condition for collection : " + condition);
        }
        Object[] entries = compiledCondition.getParameters().values().toArray();
        Object[] attributeKeys = conditionParameterMap.keySet().toArray();
        Object[] keys = compiledCondition.getParameters().keySet().toArray();
        int paramCounter = 0;
        //int attributeCounter = attributeKeys.length - 1;
        int attributeCounter = 0;
        //int constantCounter = keys.length;
        while (paramCounter < keys.length) {
            if (entries[paramCounter] instanceof Constant) {
                Constant value = (Constant) entries[paramCounter];
                if (value.getType() == Attribute.Type.STRING) {
                    condition = condition.replaceFirst("\\?", "'" + value.getValue().toString() + "'");
                } else {
                    condition = condition.replaceFirst("\\?", value.getValue().toString());
                }

            } else {

                Object key = attributeKeys[attributeCounter];
                Object value = conditionParameterMap.get(key);
                if (value.getClass().getName().equals("java.lang.String")) {
                    condition = condition.replaceFirst("\\?", "'" + value.toString() + "'");
                } else {
                    condition = condition.replaceFirst("\\?", value.toString());
                }
                attributeCounter++;
            }
            paramCounter++;
        }

        if (log.isDebugEnabled()) {
            log.debug("Resolved condition for collection : " + condition);
        }
        return condition;
    }

    /**
     * Utility method tp map the values to the respective attributes before database writes.
     *
     * @param record         Object array of the runtime values.
     * @param attributeNames List containing names of the attributes.
     * @return Document
     */
    public static Map<String, Object> mapValuesToAttributes(Object[] record, List<String> attributeNames) {
        Map<String, Object> attributesValuesMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValuesMap.put(attributeNames.get(i), record[i]);
        }
        return attributesValuesMap;
    }

}
