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

/**
 * Class which holds the constants required by the CosmosDB Event Table implementation.
 */
public class CosmosTableConstants {

    //Annotation field names
    public static final String ANNOTATION_ELEMENT_URI = "uri";
    public static final String ANNOTATION_ELEMENT_ACCESS_KEY = "access.key";
    public static final String ANNOTATION_ELEMENT_DATABASE_NAME = "database.name";
    public static final String ANNOTATION_ELEMENT_COLLECTION_NAME = "collection.name";

    //Miscellaneous SQL constants
    public static final String SQL_MATH_ADD = "+";
    public static final String SQL_MATH_DIVIDE = "/";
    public static final String SQL_MATH_MULTIPLY = "*";
    public static final String SQL_MATH_SUBTRACT = "-";
    public static final String SQL_MATH_MOD = "%";
    public static final String SQL_COMPARE_LESS_THAN = "<";
    public static final String SQL_COMPARE_GREATER_THAN = ">";
    public static final String SQL_COMPARE_LESS_THAN_EQUAL = "<=";
    public static final String SQL_COMPARE_GREATER_THAN_EQUAL = ">=";
    public static final String SQL_COMPARE_EQUAL = "=";
    public static final String SQL_COMPARE_NOT_EQUAL = "!=";
    public static final String SQL_AND = " AND ";
    public static final String SQL_OR = "OR";
    public static final String SQL_NOT = " NOT";
    public static final String SQL_IN = "IN";
    public static final String SQL_IS_NULL = "= null";
    public static final String SQL_AS = " AS ";
    public static final String SQL_MAX = "MAX"; // Used for incrementalAggregator:last()
    public static final String WHITESPACE = " ";
    public static final String EQUALS = "=";
    public static final String OPEN_PARENTHESIS = "(";
    public static final String CLOSE_PARENTHESIS = ")";
    public static final String SUB_SELECT_QUERY_REF = "t2";

    //Configurable System Parameters associated with Connection Policy
    public static final String CONNECTION_MODE = "connectionMode";
    public static final String DIRECT_REQUEST_TIMEOUT = "directRequestTimeout";
    public static final String ENABLE_ENDPOINT_DISCOVERY = "enableEndpointDiscovery";
    public static final String HANDLE_SERVICE_UNAVAILABLE_FROM_PROXY = "handleServiceUnavailableFromProxy";
    public static final String IDLE_CONNECTION_TIMEOUT = "idleConnectionTimeout";
    public static final String MAX_POOL_SIZE = "maxPoolSize";
    public static final String MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS = "MaxRetryAttemptsOnThrottledRequests";
    public static final String MAX_RETRY_WAIT_TIME = "MaxRetryWaitTimeInSeconds";
    public static final String MEDIA_READ_MODE = "mediaReadMode";
    public static final String MEDIA_REQUEST_TIMEOUT = "mediaRequestTimeout";
    public static final String PREFERRED_LOCATIONS = "preferredLocations";
    public static final String USER_AGENT_SUFFIX = "userAgentSuffix";
    public static final String USING_MULTIPLE_WRITE_LOCATIONS = "usingMultipleWriteLocations";
    public static final String REQUEST_TIMEOUT = "requestTimeout";

    public static final String CONSISTENCY_LEVEL = "consistencyLevel";

    private CosmosTableConstants() {
    }

}
