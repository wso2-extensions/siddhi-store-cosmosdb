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

    //Configurable parameters associated with request options
    public static final String ANNOTATION_ELEMENT_CUSTOM_REQUEST_OPTIONS = "custom.request.options";
    public static final String ANNOTATION_ELEMENT_SCRIPT_LOGGING = "is.script.logging.enabled";
    public static final String ANNOTATION_ELEMENT_POPULATE_QUOTA = "is.populate.quota.info";
    public static final String ANNOTATION_ELEMENT_RU_PER_MINUTE =  "is.disable.ru.per.minute.usage";
    public static final String ANNOTATION_ELEMENT_ENABLE_RU_THROUGHPUT = "offer.enable.ru.per.minute.throughput";
    public static final String ANNOTATION_ELEMENT_POPULATE_PK_STATS = "is.populate.partition.key.range.statistics";
    public static final String ANNOTATION_ELEMENT_PARTITION_KEY = "partition.key";
    public static final String ANNOTATION_ELEMENT_OFFER_THROUGHPUT = "offer.throughput";
    public static final String ANNOTATION_ELEMENT_OFFER_TYPE = "offer.type";
    public static final String ANNOTATION_ELEMENT_RESOURCE_TOKEN_EXPIRY = "resource.token.expiry.seconds";
    public static final String ANNOTATION_ELEMENT_SESSION_TOKEN = "session.token";
    public static final String ANNOTATION_ELEMENT_INDEXING_DIRECTIVE = "indexing.directive";
    public static final String ANNOTATION_ELEMENT_ACCESS_CONDITION_TYPE = "access.condition.type";
    public static final String ANNOTATION_ELEMENT_ACCESS_CONDITION = "access.condition";
    public static final String ANNOTATION_ELEMENT_PRE_TRIGGER_INCLUDE = "pre.trigger.include";
    public static final String ANNOTATION_ELEMENT_POST_TRIGGER_INCLUDE = "post.trigger.include";
    public static final String ANNOTATION_ELEMENT_ID_GENERATION = "disable.automatic.id.generation";

    //Configurable parameters associated with feed options
    public static final String ANNOTATION_ELEMENT_PARTITION_KEY_RANGE_ID = "partition.key.range.id";
    public static final String ANNOTATION_ELEMENT_ENABLE_SCAN_IN_QUERY = "is.enable.scan.in.query";
    public static final String ANNOTATION_ELEMENT_EMIT_VERBOSE_TRACES_IN_QUERY = "is.emit.verbose.traces.in.query";
    public static final String ANNOTATION_ELEMENT_ENABLE_CROSS_PARTITION_QUERY = "is.enable.cross.partition.query";
    public static final String ANNOTATION_ELEMENT_MAX_DEGREE_OF_PARALLELISM = "max.degree.of.parallelism";
    public static final String ANNOTATION_ELEMENT_MAX_BUFFERED_ITEM_COUNT = "max.buffered.item.count";
    public static final String ANNOTATION_ELEMENT_RESPONSE_TOKEN_LIMIT = "response.continuation.token.limit.in.kb";

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

    //SQL queries
    public static final String SQL_SELECT_FROM_ROOT = "SELECT * FROM root r WHERE r.id=?";
    public static final String SQL_SELECT = "SELECT * FROM ? WHERE ?";

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
