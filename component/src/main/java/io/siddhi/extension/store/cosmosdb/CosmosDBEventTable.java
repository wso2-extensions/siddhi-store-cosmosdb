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

package io.siddhi.extension.store.cosmosdb;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.cosmosdb.util.CosmosTableConstants;
import io.siddhi.extension.store.cosmosdb.util.CosmosTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;


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
                                "cosmosdb://[username:password@]host1[:port1][,hostN[:portN]][/[database][?options]]\n"
                                + "The options specified in the uri will override any connection options specified in "
                                + "the deployment yaml file.",
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
    private String collectionId = "Items";
    private List<String> attributes;
    private String tableName = this.collectionId;


    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList());

        Annotation storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());


        this.initializeConnectionParameters(storeAnnotation, configReader);

        String customCollectionName =
                storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_COLLECTION_NAME);
        this.collectionId = CosmosTableUtils.isEmpty(customCollectionName) ? tableDefinition.getId() :
                customCollectionName;
        this.initialCollectionTest = false;
    }

    /**
     * Method for initializing HOST and database name.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     * @param configReader    {@link ConfigReader} ConfigurationReader.
     */
    private void initializeConnectionParameters(Annotation storeAnnotation, ConfigReader configReader) {
        String host = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_URI);
        String masterKey = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_MASTER_KEY);

        if (documentClient == null) {
            documentClient = new DocumentClient(host, masterKey, ConnectionPolicy.GetDefault(),
                    ConsistencyLevel.Session);
        }


    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {

        for (Object[] record : records) {
            Map<String, Object> insertMap = CosmosTableUtils.mapValuesToAttributes(record, this.attributeNames);
            Document insertDocument = new Document(gson.toJson(insertMap));

            try {
                // Persist the document using the DocumentClient.
                documentClient.createDocument(getCollectionId().getSelfLink(), insertDocument, null, false);
            } catch (DocumentClientException e) {
                log.error("Failed to add document", e);
            }
        }
    }


    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        List<Document> documentList = null;
        try {
            documentList = queryDocuments((CosmosCompiledCondition) compiledCondition, findConditionParameterMap);
        } catch (SQLException e) {
            log.error("Failed to find document", e);
        }
        if (documentList != null) {
            return new CosmosIterator(documentList, this.attributeNames);
        } else {
            return null;
        }
    }


    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        List<Document> documentList = null;
        try {
            documentList = queryDocuments((CosmosCompiledCondition) compiledCondition, containsConditionParameterMap);
        } catch (SQLException e) {
            log.error("Failed to find document", e);
        }
        if (documentList != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
    }

    private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps,
                                    CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        try {
            for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
                List<Document> documentList = queryDocuments((CosmosCompiledCondition) compiledCondition,
                        deleteConditionParameterMap);
                for (Document toDeleteDocument : documentList) {
                    try {
                        documentClient.deleteDocument(toDeleteDocument.getSelfLink(), null);
                    } catch (DocumentClientException e) {
                        log.error("Failed to delete document", e);
                    }
                }
            }

        } catch (SQLException e) {
            log.error("Failed to find document", e);
        }
    }

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list, Map<String,
            CompiledExpression> map, List<Map<String, Object>> list1) throws ConnectionUnavailableException {
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> updateConditionParameterMap = null;
            for (Map<String, Object> stringObjectMap : list) {
                updateConditionParameterMap = stringObjectMap;
            }
            int ordinal = list.indexOf(updateConditionParameterMap);
            List<Document> documentList = null;
            try {
                documentList = queryDocuments((CosmosCompiledCondition) compiledCondition, updateConditionParameterMap);
            } catch (SQLException e) {
                log.error("Failed to find document", e);
            }

            if (documentList != null) {
                for (Document toUpdateDocument : documentList) {
                    try {
                        for (String key : list1.get(ordinal).keySet()) {
                            Object value = list1.get(ordinal).get(key);
                            toUpdateDocument.set(key, value);
                        }
                        documentClient.replaceDocument(toUpdateDocument, null);

                    } catch (DocumentClientException e) {
                        log.error("Failed to update document", e);
                    }
                }
            }
        }
    }

    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list, Map<String,
            CompiledExpression> map, List<Map<String, Object>> list1, List<Object[]> list2)
            throws ConnectionUnavailableException {
        Map<String, Object> updateOrAddConditionParameterMap = null;
        for (Map<String, Object> stringObjectMap : list) {
            updateOrAddConditionParameterMap = stringObjectMap;
        }
        int ordinal = list.indexOf(updateOrAddConditionParameterMap);
        List<Document> documentList = null;
        try {
            documentList = queryDocuments((CosmosCompiledCondition) compiledCondition,
                    updateOrAddConditionParameterMap);
        } catch (SQLException e) {
            log.error("Failed to find document", e);
        }
        //update
        if (documentList != null) {
            for (Document toUpdateDocument : documentList) {
                try {
                    for (String key : list1.get(ordinal).keySet()) {
                        Object value = list1.get(ordinal).get(key);
                        toUpdateDocument.set(key, value);
                    }
                    documentClient.replaceDocument(toUpdateDocument, null);
                } catch (DocumentClientException e) {
                    log.error("Failed to update document", e);
                }
            }
        } else {
            add(list2);
        }
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CosmosConditionVisitor visitor = new CosmosConditionVisitor(this.collectionId, false);
        expressionBuilder.build(visitor);
        return new CosmosCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.isContainsConditionExist(), visitor.getOrdinalOfContainPattern(), false, null, null,
                expressionBuilder.getUpdateOrInsertReducer(), expressionBuilder.getInMemorySetExpressionExecutor());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }


    private Database getDatabaseId() {
        if (databaseCache == null) {
            // Get the database if it exists
            List<Database> databaseList =
                    documentClient.queryDatabases("SELECT * FROM root r WHERE r.id='" + databaseId + "'",
                            null).getQueryIterable().toList();

            if (databaseList.size() > 0) {
                // Cache the database object so we won't have to query for it
                // later to retrieve the selfLink.
                databaseCache = databaseList.get(0);
            } else {
                // Create the database if it doesn't exist.
                try {
                    Database databaseDefinition = new Database();
                    databaseDefinition.setId(databaseId);

                    databaseCache = documentClient.createDatabase(databaseDefinition, null).getResource();
                } catch (DocumentClientException e) {
                    log.error("Failed to create the database", e);
                }
            }
        }

        return databaseCache;
    }

    private DocumentCollection getCollectionId() {
        if (collectionCache == null) {
            // Get the collection if it exists.
            List<DocumentCollection> collectionList = documentClient.queryCollections(getDatabaseId().getSelfLink(),
                    "SELECT * FROM root r WHERE r.id='" + collectionId + "'", null).getQueryIterable().toList();

            if (collectionList.size() > 0) {
                // Cache the collection object so we won't have to query for it
                // later to retrieve the selfLink.
                collectionCache = collectionList.get(0);
            } else {
                // Create the collection if it doesn't exist.
                try {
                    DocumentCollection collectionDefinition = new DocumentCollection();
                    collectionDefinition.setId(collectionId);

                    collectionCache = documentClient.createCollection(getDatabaseId().getSelfLink(),
                            collectionDefinition, null).getResource();
                } catch (DocumentClientException e) {
                    log.error("Failed to create the collection", e);
                }
            }
        }

        return collectionCache;
    }

    private List<Document> queryDocuments(CosmosCompiledCondition compiledCondition,
                                          Map<String, Object> conditionParameterMap) throws SQLException {
        String condition = CosmosTableUtils.resolveCondition(compiledCondition, conditionParameterMap);
        SqlQuerySpec query = new SqlQuerySpec();
        query.setQueryText("SELECT * FROM " + collectionId + " WHERE " + condition);
        FeedOptions options = new FeedOptions();
        options.setEnableScanInQuery(true);

        return documentClient.queryDocuments(getCollectionId().getSelfLink(),
                query, options).getQueryIterable().toList();
    }


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

