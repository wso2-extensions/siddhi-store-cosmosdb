/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.store.cosmosdb.util;

/**
 * Class which holds the constants required by the CosmosDB Event Table implementation.
 */
public class CosmosTableConstants {

    //Annotation field names
    public static final String ANNOTATION_ELEMENT_URI = "cosmosdb.uri";
    public static final String ANNOTATION_ELEMENT_MASTERKEY = "cosmosdb.key";
    public static final String ANNOTATION_ELEMENT_COLLECTION_NAME = "collection.name";


    //Placeholders for condition replacements
    public static final String PLACEHOLDER_LEFT_OPERAND = "{{LEFT_OPERAND}}";
    public static final String PLACEHOLDER_RIGHT_OPERAND = "{{RIGHT_OPERAND}}";
    public static final String PLACEHOLDER_OPERAND = "{{OPERAND}}";
    public static final String PLACEHOLDER_FIELD_NAME = "{{FIELD_NAME}}";
    public static final String PLACEHOLDER_COMPARE_OPERATOR = "{{COMPARE_OPERATOR}}";

    public static final String VARIABLE_CARBON_HOME = "carbon.home";


    //Copied from CosmosDB
    //Placeholder strings needed for processing the query configuration file
    //public static final String CosmosDB_QUERY_CONFIG_FILE = "cosmosdb-table-config.xml";
    public static final String PLACEHOLDER_COLUMNS_FOR_CREATE = "{{COLUMNS, PRIMARY_KEYS}}";
    public static final String PLACEHOLDER_CONDITION = "{{CONDITION}}";
    public static final String PLACEHOLDER_COLUMNS_VALUES = "{{COLUMNS_AND_VALUES}}";
    public static final String PLACEHOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    public static final String PLACEHOLDER_INDEX = "{{INDEX_COLUMNS}}";
    public static final String PLACEHOLDER_Q = "{{Q}}";
    public static final String PLACEHOLDER_COLUMNS = "{{COLUMNS}}";
    public static final String PLACEHOLDER_VALUES = "{{VALUES}}";
    public static final String PLACEHOLDER_SELECTORS = "{{SELECTORS}}";
    public static final String PLACEHOLDER_INNER_QUERY = "{{INNER_QUERY}}";
    public static final String PLACEHOLDER_LIMIT_WRAPPER = "{{LIMIT_WRAPPER}}";
    public static final String PLACEHOLDER_OFFSET_WRAPPER = "{{OFFSET_WRAPPER}}";
    public static final String PLACEHOLDER_INDEX_NUMBER = "{{INDEX_NUM}}";

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
    public static final String SQL_NOT = "NOT";
    public static final String SQL_IN = "IN";
    public static final String SQL_IS_NULL = "IS NULL";
    public static final String SQL_NOT_NULL = "NOT NULL";
    public static final String SQL_PRIMARY_KEY_DEF = "PRIMARY KEY";
    public static final String SQL_WHERE = "WHERE";
    public static final String SQL_AS = " AS ";
    public static final String SQL_MAX = "MAX"; // Used for incrementalAggregator:last()
    public static final String WHITESPACE = " ";
    public static final String SEPARATOR = ", ";
    public static final String EQUALS = "=";
    public static final String QUESTION_MARK = "?";
    public static final String OPEN_PARENTHESIS = "(";
    public static final String CLOSE_PARENTHESIS = ")";
    public static final String SUB_SELECT_QUERY_REF = "t2";

    public static final String CONTAINS_CONDITION_REGEX = "(CONTAINS\\()([a-zA-z.]*)(\\s\\?\\s\\))";


    //Check these again
    public static final String PROPERTY_SEPARATOR = ".";
    public static final String RECORD_INSERT_QUERY = "INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})";
    public static final String RECORD_UPDATE_QUERY = "UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}";
    public static final String RECORD_SELECT_QUERY = "SELECT * FROM {{TABLE_NAME}} {{CONDITION}}";
    public static final String RECORD_EXISTS_QUERY = "SELECT TOP 1 FROM {{TABLE_NAME}} {{CONDITION}}";
    public static final String RECORD_DELETE_QUERY = "DELETE FROM {{TABLE_NAME}} {{CONDITION}}";
    public static final String SELECT_CLAUSE = "SELECT {{SELECTORS}} FROM {{TABLE_NAME}}";
    public static final String SELECT_QUERY_WITH_SUB_SELECT_TEMPLATE = "SELECT {{SELECTORS}} FROM {{TABLE_NAME}}, ( {{INNER_QUERY}} ) AS t2";
    public static final String WHERE_CLAUSE = "WHERE {{CONDITION}}";
    public static final String ORDER_BY_CLAUSE = "ORDER BY {{COLUMNS}}";
    public static final String LIMIT_CLAUSE = "LIMIT {{Q}}";
    public static final String OFFSET_CLAUSE = "OFFSET {{Q}}";
    public static final boolean IS_LIMIT_BEFORE_OFFSET = false;


    private CosmosTableConstants() {
    }
}
