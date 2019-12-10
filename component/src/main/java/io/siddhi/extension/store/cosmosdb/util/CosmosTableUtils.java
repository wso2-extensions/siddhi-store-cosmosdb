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

import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.AccessConditionType;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.IndexingDirective;
import com.microsoft.azure.documentdb.MediaReadMode;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.cosmosdb.CosmosCompiledCondition;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

import static io.siddhi.query.api.definition.Attribute.Type;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

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
     */
    public static String resolveCondition(CosmosCompiledCondition compiledCondition,
                                          Map<String, Object> conditionParameterMap) {

        String condition = compiledCondition.getCompiledQuery();
        if (log.isDebugEnabled()) {
            log.debug("compiled condition for collection : " + condition);
        }
        SortedMap<Integer, Object> parameters = compiledCondition.getParameters();
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            if (entry.getValue() instanceof Constant) {
                Constant value = (Constant) entry.getValue();
                if (value.getType() == STRING) {
                    condition = condition.replaceFirst("\\?",
                            "'" + value.getValue().toString() + "'");
                } else {
                    condition = condition.replaceFirst("\\?", value.getValue().toString());
                }
            } else {
                Attribute replacingAttribute = (Attribute) entry.getValue();
                Type type = replacingAttribute.getType();
                if (type == STRING) {
                    condition = condition.replaceFirst("\\?",
                            "'" + conditionParameterMap.get(replacingAttribute.getName()).toString() + "'");
                } else {
                    condition = condition.replaceFirst("\\?",
                            String.valueOf(conditionParameterMap.get(replacingAttribute.getName())));
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Resolved condition for collection : " + condition);
        }
        return condition;
    }

    public static ConnectionPolicy getConnectionPolicy(ConfigReader configReader) {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();

        connectionPolicy.setRequestTimeout(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.REQUEST_TIMEOUT, String.valueOf(
                        ConnectionPolicy.GetDefault().getRequestTimeout()))));
        connectionPolicy.setDirectRequestTimeout(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.DIRECT_REQUEST_TIMEOUT, String.valueOf(
                        ConnectionPolicy.GetDefault().getDirectRequestTimeout()))));
        connectionPolicy.setMediaRequestTimeout(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.MEDIA_REQUEST_TIMEOUT, String.valueOf(
                        ConnectionPolicy.GetDefault().getMediaRequestTimeout()))));
        connectionPolicy.setConnectionMode(ConnectionMode.valueOf(configReader.readConfig(
                CosmosTableConstants.CONNECTION_MODE, String.valueOf(
                        ConnectionPolicy.GetDefault().getConnectionMode()))));
        connectionPolicy.setMediaReadMode(MediaReadMode.valueOf(configReader.readConfig(
                CosmosTableConstants.MEDIA_READ_MODE, String.valueOf(
                        ConnectionPolicy.GetDefault().getMediaReadMode()))));
        connectionPolicy.setMaxPoolSize(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.MAX_POOL_SIZE, String.valueOf(ConnectionPolicy.GetDefault().getMaxPoolSize()))));
        connectionPolicy.setIdleConnectionTimeout(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.IDLE_CONNECTION_TIMEOUT, String.valueOf(
                        ConnectionPolicy.GetDefault().getIdleConnectionTimeout()))));
        connectionPolicy.setUserAgentSuffix(configReader.readConfig(
                CosmosTableConstants.USER_AGENT_SUFFIX, ConnectionPolicy.GetDefault().getUserAgentSuffix()));
        connectionPolicy.setEnableEndpointDiscovery(Boolean.parseBoolean(configReader.readConfig(
                CosmosTableConstants.ENABLE_ENDPOINT_DISCOVERY, String.valueOf(
                        ConnectionPolicy.GetDefault().getEnableEndpointDiscovery()))));
        connectionPolicy.setPreferredLocations(Collections.singleton((String.valueOf(configReader.readConfig(
                CosmosTableConstants.PREFERRED_LOCATIONS, String.valueOf(
                        ConnectionPolicy.GetDefault().getPreferredLocations()))))));
        connectionPolicy.setUsingMultipleWriteLocations(Boolean.parseBoolean(configReader.readConfig(
                CosmosTableConstants.USING_MULTIPLE_WRITE_LOCATIONS, String.valueOf(
                        ConnectionPolicy.GetDefault().isUsingMultipleWriteLocations()))));
        connectionPolicy.setHandleServiceUnavailableFromProxy(Boolean.parseBoolean(configReader.readConfig(
                CosmosTableConstants.HANDLE_SERVICE_UNAVAILABLE_FROM_PROXY, String.valueOf(
                        ConnectionPolicy.GetDefault().getHandleServiceUnavailableFromProxy()))));
        connectionPolicy.getRetryOptions().setMaxRetryWaitTimeInSeconds(Integer.parseInt(configReader.readConfig(
                CosmosTableConstants.MAX_RETRY_WAIT_TIME, String.valueOf(
                        ConnectionPolicy.GetDefault().getRetryOptions().getMaxRetryWaitTimeInSeconds()))));
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(Integer.parseInt(
                configReader.readConfig(CosmosTableConstants.MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS, String.valueOf(
                        ConnectionPolicy.GetDefault().getRetryOptions().getMaxRetryAttemptsOnThrottledRequests()))));

        return connectionPolicy;
    }

    public static RequestOptions getCustomRequestOptions(Annotation storeAnnotation) {
        RequestOptions requestOptions = new RequestOptions();
        AccessCondition accessCondition = new AccessCondition();

        requestOptions.setScriptLoggingEnabled(Boolean.parseBoolean(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_SCRIPT_LOGGING)));
        requestOptions.setPopulateQuotaInfo(Boolean.parseBoolean(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_POPULATE_QUOTA)));
        requestOptions.setDisableRUPerMinuteUsage(Boolean.parseBoolean(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_RU_PER_MINUTE)));
        requestOptions.setOfferEnableRUPerMinuteThroughput(Boolean.parseBoolean(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_ENABLE_RU_THROUGHPUT)));
        requestOptions.setPopulatePartitionKeyRangeStatistics(Boolean.parseBoolean(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_POPULATE_PK_STATS)));
        requestOptions.setPartitionKey(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_PARTITION_KEY)) ? null : PartitionKey.FromJsonString(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_PARTITION_KEY)));
        requestOptions.setOfferThroughput(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_OFFER_THROUGHPUT)) ? null : Integer.valueOf(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_OFFER_THROUGHPUT)));
        requestOptions.setOfferType(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_OFFER_TYPE)) ? null : storeAnnotation.getElement(
                        CosmosTableConstants.ANNOTATION_ELEMENT_OFFER_TYPE));
        requestOptions.setResourceTokenExpirySeconds(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_RESOURCE_TOKEN_EXPIRY)) ? null : Integer.valueOf(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_RESOURCE_TOKEN_EXPIRY)));
        requestOptions.setSessionToken(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_SESSION_TOKEN)) ? null : storeAnnotation.getElement(
                        CosmosTableConstants.ANNOTATION_ELEMENT_SESSION_TOKEN));
        requestOptions.setIndexingDirective(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_INDEXING_DIRECTIVE)) ? null : IndexingDirective.valueOf(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_INDEXING_DIRECTIVE)));
        accessCondition.setType(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_ACCESS_CONDITION_TYPE)) ? null :
                AccessConditionType.valueOf(storeAnnotation.getElement(
                        CosmosTableConstants.ANNOTATION_ELEMENT_ACCESS_CONDITION_TYPE)));
        accessCondition.setCondition(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_ACCESS_CONDITION)) ? null : storeAnnotation.getElement(
                        CosmosTableConstants.ANNOTATION_ELEMENT_ACCESS_CONDITION));
        requestOptions.setAccessCondition(accessCondition);
        requestOptions.setPreTriggerInclude(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_PRE_TRIGGER_INCLUDE)) ? null : Collections.singletonList(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_PRE_TRIGGER_INCLUDE)));
        requestOptions.setPostTriggerInclude(isEmpty(storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_POST_TRIGGER_INCLUDE)) ? null : Collections.singletonList(
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_POST_TRIGGER_INCLUDE)));

        String customOptions = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_REQUEST_OPTIONS);
        if (customOptions != null) {
            String[] customOption = customOptions.split(",");
            for (int counter = 0; counter < customOption.length; counter++) {
                String[] option = customOption[counter].split(":");
                String key = option[0];
                String value = option[1];
                requestOptions.setCustomRequestOption(key, value);
            }
        }
        return requestOptions;
    }
}
