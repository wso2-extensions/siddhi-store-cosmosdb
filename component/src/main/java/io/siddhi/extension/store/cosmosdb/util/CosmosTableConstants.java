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
    public static final String ANNOTATION_ELEMENT_URI = "cosmosdb.uri";
    public static final String ANNOTATION_ELEMENT_MASTER_KEY = "cosmosdb.key";
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
    public static final String SQL_COMPARE_NOT_EQUAL = "<>"; //Using the ANSI SQL-92 standard over '!=' (non-standard)
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

    private CosmosTableConstants() {
    }
}
