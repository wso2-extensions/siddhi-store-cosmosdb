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
                "in a CosmosDB of user's choice.", //TODO more descriptive
        parameters = {
                @Parameter(name = "uri",
                        description = "The CosmosDB URI for the CosmosDB data store. The uri must be of the format \n" +
                                "https://{databaseaccount}.documents.azure.com/dbs/{db}",
                        type = {DataType.STRING}),
                @Parameter(name = "access.key",
                        description = "The CosmosDB Access key for the CosmosDB data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "database.name",
                        description = "The name of the CosmosDB database containing this event table",
                        //TODO add more description
                        type = {DataType.STRING}),
                @Parameter(name = "collection.name",
                        description = "The name of the collection in the store this Event Table should" +
                                " be persisted as.", //TODO add more description
                        optional = true,
                        defaultValue = "Name of the siddhi event table.",
                        type = {DataType.STRING})
        },
        systemParameter = {
                @SystemParameter(name = "requestTimeout",
                        description = "Sets the request timeout (time to wait for response from network peer) in " +
                                "seconds.",
                        defaultValue = "60",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "directRequestTimeout",
                        description = "Sets the direct mode request timeout (time to wait for response from network " +
                                "peer) in seconds. This only applies to requests that talk directly to the backend. ",
                        defaultValue = "5",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "mediaRequestTimeout",
                        description = "Sets time to wait for response from network peer for attachment content " +
                                "(aka media) operations in seconds.",
                        defaultValue = "300",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "connectionMode",
                        description = "Sets the connection mode used in the client. Direct and Gateway connectivity " +
                                "modes are supported",
                        defaultValue = "Gateway",
                        possibleParameters = {"Gateway", "DirectHttps"}),
                @SystemParameter(name = "mediaReadMode",
                        description = "Sets the attachment content (aka media) download mode.",
                        defaultValue = "Buffered",
                        possibleParameters = {"Buffered", "Streamed"}),
                @SystemParameter(name = "maxPoolSize",
                        description = "Sets the value of the connection pool size of the httpclient.",
                        defaultValue = "100",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "idleConnectionTimeout",
                        description = "Sets the value of the timeout for an idle connection in seconds. After that " +
                                "time, the connection will be automatically closed.",
                        defaultValue = "60",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "userAgentSuffix",
                        description = "Sets the value to be appended to the user-agent header, this is used for " +
                                "monitoring purposes",
                        defaultValue = "\"\"",
                        possibleParameters = "any String value"),
                @SystemParameter(name = "MaxRetryAttemptsOnThrottledRequests",
                        description = "Sets the maximum number of retries in the case where the request fails " +
                                "because the service has applied rate limiting on the client. The default value is " +
                                "9. This means in the case where the request is throttled, the same request will be " +
                                "issued for a maximum of 10 times to the server before an error is returned to " +
                                "the application.",
                        defaultValue = "9",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "MaxRetryWaitTimeInSeconds",
                        description = "Sets the maximum retry time in seconds. When a request fails due to a " +
                                "throttle error, the service sends back a response that contains a value indicating " +
                                "the client should not retry before the time period has elapsed (Retry-After). " +
                                "The MaxRetryWaitTime flag allows the application to set a maximum wait time for " +
                                "all retry attempts.",
                        defaultValue = "30",
                        possibleParameters = "any positive integer"),
                @SystemParameter(name = "enableEndpointDiscovery",
                        description = "Sets the flag to enable endpoint discovery for geo-replicated database " +
                                "accounts. When EnableEndpointDiscovery is true, the extension will automatically " +
                                "discover the current write and read regions to ensure requests are sent to the " +
                                "correct region based on the capability of the region and the user's preference.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "preferredLocations",
                        description = "Sets the preferred locations for geo-replicated database accounts. For " +
                                "example, \"East US\" as the preferred location. If EnableEndpointDiscovery is set " +
                                "to false, this property is ignored.",
                        defaultValue = "null",
                        possibleParameters = "list of valid locations"),
                @SystemParameter(name = "usingMultipleWriteLocations",
                        description = "Sets the value to enable writes on any locations (regions) for geo-replicated " +
                                "database accounts. When the value of this property is true, the SDK will direct " +
                                "write operations to available writable locations of geo-replicated database " +
                                "account. Writable locations are ordered by PreferredLocations property. Setting the " +
                                "property value to true has no effect until EnableMultipleWriteLocations in " +
                                "DatabaseAccount is also set to true.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "handleServiceUnavailableFromProxy",
                        description = "Sets the value to handle service unavailable errors returned without a " +
                                "service version header, by a proxy. When the value of this property is true, the " +
                                "extension will handle it as a known error and perform retries.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "consistencyLevel",
                        description = "Represents the consistency levels supported for Azure Cosmos DB client " +
                                "operations in the Azure Cosmos DB database service. The requested ConsistencyLevel " +
                                "must match or be weaker than that provisioned for the database account. Consistency " +
                                "levels by order of strength are Strong, BoundedStaleness, Session and Eventual.",
                        defaultValue = "Session",
                        possibleParameters = {"Strong", "BoundedStaleness", "Session", "Eventual", "ConsistentPrefix"})
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"cosmosdb\"," +
                                "uri=\"https://myCosmosDBName.documents.azure.com:443\", \n" +
                                "access.key=\"someAutogeneratedKey\", \n " +
                                //TODO change to access key generated by the cosmosdb
                                "database.name=\"admin\") \n" +
                                "define table FooTable (symbol string, price float, volume long);",
                        description = "This will create a collection called FooTable for the events to be saved."
                        //TODO add examples for CRUD
                )
        }
)
public class CosmosDBEventTable extends AbstractRecordTable {
    private static final Log log = LogFactory.getLog(CosmosDBEventTable.class);
    private Gson gson = new Gson();
    private DocumentClient documentClient;
    private Database databaseCache;
    private DocumentCollection collectionCache;
    private List<String> attributeNames;
    private String databaseId;
    private String collectionId;


    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList());

        Annotation storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        this.createDocumentClient(storeAnnotation, configReader);
        this.databaseId = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_DATABASE_NAME);

        String customCollectionName =
                storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_COLLECTION_NAME);

        this.collectionId = CosmosTableUtils.isEmpty(customCollectionName) ? tableDefinition.getId() :
                customCollectionName;

    }

    /**
     * Method for initializing HOST and database name.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     */
    private void createDocumentClient(Annotation storeAnnotation, ConfigReader configReader) {
        String uri = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_URI);
        String accessKey = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_ACCESS_KEY);
//TODO validate host and accessKey

        ConnectionPolicy connectionPolicy = CosmosTableUtils.getConnectionPolicy(configReader);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(configReader.readConfig(
                CosmosTableConstants.CONSISTENCY_LEVEL, String.valueOf(ConsistencyLevel.Session)));

        if (documentClient == null) {
            documentClient = new DocumentClient(uri, accessKey, connectionPolicy,
                    consistencyLevel);
        }
    }

    @Override
    protected void add(List<Object[]> records) {

        for (Object[] record : records) {
            Map<String, Object> insertMap = CosmosTableUtils.mapValuesToAttributes(record, this.attributeNames);
            Document insertDocument = new Document(gson.toJson(insertMap)); //TODO check Json map

            try {
                documentClient.createDocument(getCollectionId().getSelfLink(), insertDocument, null, false);
            } catch (DocumentClientException e) { //TODO check options and idGeneration
                log.error("Failed to add document. ", e);
            }
        }
    }


    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) {
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
                               CompiledCondition compiledCondition) {
        List<Document> documentList = null;
        try {
            documentList = queryDocuments((CosmosCompiledCondition) compiledCondition, containsConditionParameterMap);
        } catch (SQLException e) {
            log.error("Failed to find document", e);
        }
        if (documentList != null) {
            return documentList.size() > 0;
        } else {
            return false;
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) {
        this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
    }

    private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps,
                                    CompiledCondition compiledCondition) {
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
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> updateSetParameterMaps) {
        for (int i = 0; i < updateConditionParameterMaps.size(); i++) {
            Map<String, Object> updateConditionParameterMap = null;
            for (Map<String, Object> stringObjectMap : updateConditionParameterMaps) {
                updateConditionParameterMap = stringObjectMap;
            }
            int ordinal = updateConditionParameterMaps.indexOf(updateConditionParameterMap);
            List<Document> documentList = null;
            try {
                documentList = queryDocuments((CosmosCompiledCondition) compiledCondition, updateConditionParameterMap);
            } catch (SQLException e) {
                log.error("Failed to find document", e);
            }

            if (documentList != null) {
                for (Document toUpdateDocument : documentList) {
                    try {
                        for (String key : updateSetParameterMaps.get(ordinal).keySet()) {
                            Object value = updateSetParameterMaps.get(ordinal).get(key);
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
    protected void updateOrAdd(CompiledCondition compiledCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingDocuments) {
        for (int i = 0; i < updateConditionParameterMaps.size(); i++) {
            Map<String, Object> updateOrAddConditionParameterMap = null;
            for (Map<String, Object> stringObjectMap : updateConditionParameterMaps) {
                updateOrAddConditionParameterMap = stringObjectMap;
            }
            int ordinal = updateConditionParameterMaps.indexOf(updateOrAddConditionParameterMap);
            List<Document> documentList = null;
            try {
                documentList = queryDocuments((CosmosCompiledCondition) compiledCondition,
                        updateOrAddConditionParameterMap);
            } catch (SQLException e) {
                log.error("Failed to find document", e);
            }
            //update
            if (documentList != null) {
                if (documentList.size() > 0) {
                    for (Document toUpdateDocument : documentList) {
                        try {
                            for (String key : updateSetParameterMaps.get(ordinal).keySet()) {
                                Object value = updateSetParameterMaps.get(ordinal).get(key);
                                toUpdateDocument.set(key, value);
                            }
                            documentClient.replaceDocument(toUpdateDocument, null);
                        } catch (DocumentClientException e) {
                            log.error("Failed to update document", e);
                        }
                    }
                } else {
                    add(addingDocuments);
                }
            }
        }
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CosmosConditionVisitor visitor = new CosmosConditionVisitor(this.collectionId, false);
        expressionBuilder.build(visitor);
        return new CosmosCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
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
                    try {
                        databaseCache = documentClient.createDatabase(databaseDefinition, null).getResource();
                    } catch (ClassCastException e) {
                        log.error("Failed to cast from Request Response to Database", e);
                    }
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
                    try {
                        collectionCache = documentClient.createCollection(getDatabaseId().getSelfLink(),
                                collectionDefinition, null).getResource();
                    } catch (ClassCastException e) {
                        log.error("Failed to cast from Request Response to Document Collection", e);
                    }

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
    protected void connect() {

    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }

}

