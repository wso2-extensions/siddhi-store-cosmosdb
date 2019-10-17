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
package io.siddhi.extension.store.cosmosdb;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.*;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.cosmosdb.exception.CosmosTableException;
import io.siddhi.extension.store.cosmosdb.util.CosmosTableConstants;
import io.siddhi.extension.store.cosmosdb.util.CosmosTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static io.siddhi.extension.store.cosmosdb.util.CosmosTableConstants.*;


/**
 * Class representing CosmosDB Event Table implementation.
 */
@Extension(
        name = "cosmosdb",
        namespace = "store",
        description = "Using this extension a CosmosDB Event Table can be configured to persist events " +
                "in a CosmosDB of user's choice.",
        parameters = {
                @Parameter(name = "cosmosdb.uri",
                        description = "The CosmosDB URI for the CosmosDB data store. The uri must be of the format \n" +
                                "cosmosdb://[username:password@]host1[:port1][,hostN[:portN]][/[database][?options]]\n" +
                                "The options specified in the uri will override any connection options specified in " +
                                "the deployment yaml file.",
                        type = {DataType.STRING}),
                @Parameter(name = "cosmosdb.key",
                        description = "The CosmosDB Master key for the CosmosDB data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "collection.name",
                        description = "The name of the collection in the store this Event Table should" +
                                " be persisted as.",
                        optional = true,
                        defaultValue = "Name of the siddhi event table.",
                        type = {DataType.STRING}),
                @Parameter(name = "secure.connection",
                        description = "Describes enabling the SSL for the cosmosdb connection",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING})
        },
        systemParameter = {
                @SystemParameter(name = "applicationName",
                        description = "Sets the logical name of the application using this CosmosClient. The " +
                                "application name may be used by the client to identify the application to " +
                                "the server, for use in server logs, slow query logs, and profile collection.",
                        defaultValue = "null",
                        possibleParameters = "the logical name of the application using this CosmosClient. The " +
                                "UTF-8 encoding may not exceed 128 bytes."),
                @SystemParameter(name = "cursorFinalizerEnabled",
                        description = "Sets whether cursor finalizers are enabled.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"})
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"cosmosdb\"," +
                                "cosmosdb.uri=\"cosmosdb://admin:admin@localhost/Foo\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "@IndexBy(\"volume 1 {background:true,unique:true}\")\n" +
                                "define table FooTable (symbol string, price float, volume long);",
                        description = "This will create a collection called FooTable for the events to be saved " +
                                "with symbol as Primary Key(unique index at cosmosd level) and index for the field " +
                                "volume will be created in ascending order with the index option to create the index " +
                                "in the background.\n\n" +
                                "Note: \n" +
                                "@PrimaryKey: This specifies a list of comma-separated values to be treated as " +
                                "unique fields in the table. Each record in the table must have a unique combination " +
                                "of values for the fields specified here.\n\n" +
                                "@IndexBy: This specifies the fields that must be indexed at the database level. " +
                                "You can specify multiple values as a come-separated list. A single value to be in " +
                                "the format,\n“<FieldName> <SortOrder> <IndexOptions>”\n" +
                                "<SortOrder> - ( 1) for Ascending and (-1) for Descending\n" +
                                "<IndexOptions> - Index Options must be defined inside curly brackets. {} to be " +
                                "used for default options. Options must follow the standard cosmosdb index options " +
                                "format. Reference : " +
                                "https://docs.cosmosdb.com/manual/reference/method/db.collection.createIndex/\n" +
                                "Example : “symbol 1 {“unique”:true}”\n"
                )
        }
)
public class CosmosDBEventTable extends AbstractRecordTable {
    private static final Log log = LogFactory.getLog(CosmosDBEventTable.class);
    private static Gson gson = new Gson();
    private static DocumentClient documentClient;
    private static Database databaseCache;
    private static DocumentCollection collectionCache;
    private List<String> attributeNames;
    private boolean initialCollectionTest;
    private String databaseId = "ToDoList";
    private String collectionId = "";
    private String tableName = collectionId;


    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList());

        Annotation storeAnnotation = AnnotationHelper
                .getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());


        this.initializeConnectionParameters(storeAnnotation, configReader);

        String customCollectionName = storeAnnotation.getElement(
                CosmosTableConstants.ANNOTATION_ELEMENT_COLLECTION_NAME);
        this.collectionId = CosmosTableUtils.isEmpty(customCollectionName) ?
                tableDefinition.getId() : customCollectionName;
        this.initialCollectionTest = false;
    }

    /**
     * Method for initializing HOST and database name.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     * @param configReader    {@link ConfigReader} ConfigurationReader.
     * @throws CosmosTableException when store annotation does not contain cosmosdb.uri or contains an illegal
     *                              argument for cosmosdb.uri
     */
    private void initializeConnectionParameters(Annotation storeAnnotation, ConfigReader configReader) {
        final String HOST = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_URI);
        final String MASTER_KEY = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_MASTERKEY);

        if (documentClient == null) {
            documentClient = new DocumentClient(HOST, MASTER_KEY,
                    ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
        }


    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {

        for (int i = 0; i < records.size(); i++) {
            Map<String, Object> insertMap = CosmosTableUtils.mapValuesToAttributes(records.get(i), this.attributeNames);
            Document insertDocument = new Document(gson.toJson(insertMap));

            try {
                // Persist the document using the DocumentClient.
                documentClient.createDocument(
                        getcollectionId().getSelfLink(), insertDocument, null,
                        false).getResource();
            } catch (DocumentClientException e) {
                log.error("Failed to add record", e);
            }


        }
        /*List<InsertOneModel<Document>> parsedRecords = records.stream().map(record -> {

            return new InsertOneModel<>(insertDocument);
        }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);*/
    }


    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> map, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        return null;
    }

    @Override
    protected boolean contains(Map<String, Object> map, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        return false;
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
    }

    private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps,
                                    CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        String condition = ((CosmosCompiledCondition) compiledCondition).getCompiledQuery();
        for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
            CosmosTableUtils.resolveCondition(stmt, (CosmosCompiledCondition) compiledCondition,
                    deleteConditionParameterMap, 0);
            List<Document> documentList = documentClient
                    .queryDocuments(getcollectionId().getSelfLink(),
                            "SELECT * FROM root r WHERE '" + condition + "'", null)
                    .getQueryIterable().toList();
            if (documentList.size() > 0) {
                Document toDeleteDocument = documentList.get(0);
                try {
                    documentClient.deleteDocument(toDeleteDocument.getSelfLink(), null);
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("error");
            }
        /*PreparedStatement stmt = null;
        try {
            int counter = 0;
            for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
                CosmosTableUtils.resolveCondition(stmt, (CosmosCompiledCondition) compiledCondition,
                        deleteConditionParameterMap, 0);

            }*/
        }

    }


        /*for (Map<String, Object> conditionParams : deleteConditionParameterMaps) {
            String name = conditionParams.get(((CosmosCompiledCondition) compiledCondition)
                    .getCompiledQuery()).toString();

            List<Document> documentList = documentClient
                    .queryDocuments(getcollectionId().getSelfLink(),
                            "SELECT * FROM root r WHERE r.name='" + name + "'", null)
                    .getQueryIterable().toList();

            if (documentList.size() > 0) {
                Document toDeleteDocument = documentList.get(0);
                try {
                    documentClient.deleteDocument(toDeleteDocument.getSelfLink(), null);
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("error");
            }


        }*/





    /*private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps,
                                    CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        String condition = ((CosmosCompiledCondition) compiledCondition).getCompiledQuery();
        PreparedStatement stmt = null;
        try {
            stmt = CosmosTableUtils.isEmpty(condition) ?
                    conn.prepareStatement(deleteQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(CosmosTableUtils.formatQueryWithCondition(deleteQuery, condition));
            int counter = 0;
            for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
                CosmosTableUtils.resolveCondition(stmt, (CosmosCompiledCondition) compiledCondition,
                        deleteConditionParameterMap, 0);
                stmt.addBatch();
                counter++;
                if (counter == batchSize) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    counter = 0;
                }
            }
            if (counter > 0) {
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Error performing record deletion. Connection is closed " +
                            "for store: '" + tableName + "'", e);
                } else {
                    throw new CosmosTableException("Error performing record deletion for store '"
                            + this.tableName + "'", e);
                }
            } catch (SQLException e1) {
                throw new CosmosTableException("Error performing record deletion for store: '" + tableName + "'", e1);
            }
        } finally {
            CosmosTableUtils.cleanupConnection(null, stmt, conn);
        }
    }*/

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list, Map<String, CompiledExpression> map, List<Map<String, Object>> list1) throws ConnectionUnavailableException {

    }

    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list, Map<String, CompiledExpression> map, List<Map<String, Object>> list1, List<Object[]> list2) throws ConnectionUnavailableException {

    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CosmosConditionVisitor visitor = new CosmosConditionVisitor(this.tableName, false);
        expressionBuilder.build(visitor);
        return new CosmosCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.isContainsConditionExist(), visitor.getOrdinalOfContainPattern(), false, null, null,
                expressionBuilder.getUpdateOrInsertReducer(), expressionBuilder.getInMemorySetExpressionExecutor());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    /*@Override
    protected CompiledSelection compileSelection(List<AbstractQueryableRecordTable.SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<AbstractQueryableRecordTable.OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {
        return new CosmosCompiledSelection(
                compileSelectClause(selectAttributeBuilders),
                (groupByExpressionBuilder == null) ? null : compileClause(groupByExpressionBuilder, false),
                (havingExpressionBuilder == null) ? null :
                        compileClause(Collections.singletonList(havingExpressionBuilder), true),
                (orderByAttributeBuilders == null) ? null : compileOrderByClause(orderByAttributeBuilders),
                limit, offset);
    }*/

    private CosmosCompiledCondition compileSelectClause(List<AbstractQueryableRecordTable.SelectAttributeBuilder> selectAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        StringBuilder compiledSubSelectQuerySelection = new StringBuilder();
        StringBuilder compiledOuterOnCondition = new StringBuilder();

        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;

        boolean containsLastFunction = false;
        List<CosmosConditionVisitor> conditionVisitorList = new ArrayList<>();
        for (AbstractQueryableRecordTable.SelectAttributeBuilder attributeBuilder : selectAttributeBuilders) {
            CosmosConditionVisitor visitor = new CosmosConditionVisitor(this.tableName, false);
            attributeBuilder.getExpressionBuilder().build(visitor);
            if (visitor.isLastConditionExist()) {
                containsLastFunction = true;
            }
            conditionVisitorList.add(visitor);
        }

        boolean isLastFunctionEncountered = false;
        for (int i = 0; i < conditionVisitorList.size(); i++) {
            CosmosConditionVisitor visitor = conditionVisitorList.get(i);
            AbstractQueryableRecordTable.SelectAttributeBuilder selectAttributeBuilder = selectAttributeBuilders.get(i);

            String compiledCondition = visitor.returnCondition();

            if (containsLastFunction) {
                if (visitor.isLastConditionExist()) {
                    // Add the select columns with function incrementalAggregator:last()
                    compiledSelectionList.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    if (!isLastFunctionEncountered) {
                        //Only add max variable for incrementalAggregator:last() once
                        compiledSubSelectQuerySelection.append(visitor.returnMaxVariableCondition()).append(SEPARATOR);
                        compiledOuterOnCondition.append(visitor.getOuterCompiledCondition()).append(SQL_AND);
                        isLastFunctionEncountered = true;
                    }
                } else if (visitor.isContainsAttributeFunction()) {
                    //Add columns with attributes function such as sum(), max()
                    //Add max(variable) by default since oracle all columns not in group by must have
                    // attribute function
                    compiledSelectionList.append(SQL_MAX).append(OPEN_PARENTHESIS)
                            .append(SUB_SELECT_QUERY_REF).append(".").append(selectAttributeBuilder.getRename())
                            .append(CLOSE_PARENTHESIS).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledSubSelectQuerySelection.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                } else {
                    // Add group by column
                    compiledSelectionList.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledSubSelectQuerySelection.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledOuterOnCondition.append(visitor.getOuterCompiledCondition()).append(SQL_AND);
                }
            } else {
                compiledSelectionList.append(compiledCondition);
                if (selectAttributeBuilder.getRename() != null && !selectAttributeBuilder.getRename().isEmpty()) {
                    compiledSelectionList.append(SQL_AS).
                            append(selectAttributeBuilder.getRename());
                }
                compiledSelectionList.append(SEPARATOR);
            }

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = offset + maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }

        if (compiledSubSelectQuerySelection.length() > 0) {
            compiledSubSelectQuerySelection.setLength(compiledSubSelectQuerySelection.length() - 2);
        }

        if (compiledOuterOnCondition.length() > 0) {
            compiledOuterOnCondition.setLength(compiledOuterOnCondition.length() - 4);
        }

        return new CosmosCompiledCondition(compiledSelectionList.toString(), paramMap, false, 0, containsLastFunction,
                compiledSubSelectQuerySelection.toString(), compiledOuterOnCondition.toString(), null, null);
    }

    private CosmosCompiledCondition compileClause(List<ExpressionBuilder> expressionBuilders, boolean isHavingClause) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;

        for (ExpressionBuilder expressionBuilder : expressionBuilders) {
            CosmosConditionVisitor visitor = new CosmosConditionVisitor(this.tableName, isHavingClause);
            expressionBuilder.build(visitor);

            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition).append(SEPARATOR);

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = offset + maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new CosmosCompiledCondition(compiledSelectionList.toString(), paramMap, false,
                0, false, null, null, null, null);
    }


    private Database getdatabaseId() {
        if (databaseCache == null) {
            // Get the database if it exists
            List<Database> databaseList = documentClient
                    .queryDatabases(
                            "SELECT * FROM root r WHERE r.id='" + databaseId
                                    + "'", null).getQueryIterable().toList();

            if (databaseList.size() > 0) {
                // Cache the database object so we won't have to query for it
                // later to retrieve the selfLink.
                databaseCache = databaseList.get(0);
            } else {
                // Create the database if it doesn't exist.
                try {
                    Database databaseDefinition = new Database();
                    databaseDefinition.setId(databaseId);

                    databaseCache = documentClient.createDatabase(
                            databaseDefinition, null).getResource();
                } catch (DocumentClientException e) {
                    log.error("Failed to create the database", e);
                }
            }
        }

        return databaseCache;
    }

    private DocumentCollection getcollectionId() {
        if (collectionCache == null) {
            // Get the collection if it exists.
            List<DocumentCollection> collectionList = documentClient
                    .queryCollections(
                            getdatabaseId().getSelfLink(),
                            "SELECT * FROM root r WHERE r.id='" + collectionId
                                    + "'", null).getQueryIterable().toList();

            if (collectionList.size() > 0) {
                // Cache the collection object so we won't have to query for it
                // later to retrieve the selfLink.
                collectionCache = collectionList.get(0);
            } else {
                // Create the collection if it doesn't exist.
                try {
                    DocumentCollection collectionDefinition = new DocumentCollection();
                    collectionDefinition.setId(collectionId);

                    collectionCache = documentClient.createCollection(
                            getdatabaseId().getSelfLink(),
                            collectionDefinition, null).getResource();
                } catch (DocumentClientException e) {
                    log.error("Failed to create the collection", e);
                }
            }
        }

        return collectionCache;
    }

    /*private Document getDocumentById(String id) {
        // Retrieve the document using the DocumentClient.
        List<Document> documentList = documentClient
                .queryDocuments(getcollectionId().getSelfLink(),
                        "SELECT * FROM root r WHERE r.id='" + id + "'", null)
                .getQueryIterable().toList();

        if (documentList.size() > 0) {
            return documentList.get(0);
        } else {
            return null;
        }
    }
*/


    @Override
    protected void connect() throws ConnectionUnavailableException {

    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }

}
