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
                        type = {DataType.STRING}),
                @Parameter(name = "trust.store",
                        description = "File path to the trust store.",
                        optional = true,
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        type = {DataType.STRING}),
                @Parameter(name = "trust.store.password",
                        description = "Password to access the trust store",
                        optional = true,
                        defaultValue = "wso2carbon",
                        type = {DataType.STRING}),
                @Parameter(name = "key.store",
                        description = "File path to the keystore.",
                        optional = true,
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        type = {DataType.STRING}),
                @Parameter(name = "key.store.password",
                        description = "Password to access the keystore",
                        optional = true,
                        defaultValue = "wso2carbon",
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
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "requiredReplicaSetName",
                        description = "The name of the replica set",
                        defaultValue = "null",
                        possibleParameters = "the logical name of the replica set"),
                @SystemParameter(name = "sslEnabled",
                        description = "Sets whether to initiate connection with TSL/SSL enabled. true: Initiate " +
                                "the connection with TLS/SSL. false: Initiate the connection without TLS/SSL.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "trustStore",
                        description = "File path to the trust store.",
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        possibleParameters = "Any valid file path."),
                @SystemParameter(name = "trustStorePassword",
                        description = "Password to access the trust store",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Any valid password."),
                @SystemParameter(name = "keyStore",
                        description = "File path to the keystore.",
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        possibleParameters = "Any valid file path."),
                @SystemParameter(name = "keyStorePassword",
                        description = "Password to access the keystore",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Any valid password."),
                @SystemParameter(name = "connectTimeout",
                        description = "The time in milliseconds to attempt a connection before timing out.",
                        defaultValue = "10000",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "connectionsPerHost",
                        description = "The maximum number of connections in the connection pool.",
                        defaultValue = "100",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "minConnectionsPerHost",
                        description = "The minimum number of connections in the connection pool.",
                        defaultValue = "0",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "maxConnectionIdleTime",
                        description = "The maximum number of milliseconds that a connection can remain idle in " +
                                "the pool before being removed and closed. A zero value indicates no limit to " +
                                "the idle time.  A pooled connection that has exceeded its idle time will be " +
                                "closed and replaced when necessary by a new connection.",
                        defaultValue = "0",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "maxWaitTime",
                        description = "The maximum wait time in milliseconds that a thread may wait for a connection " +
                                "to become available. A value of 0 means that it will not wait.  A negative value " +
                                "means to wait indefinitely",
                        defaultValue = "120000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "threadsAllowedToBlockForConnectionMultiplier",
                        description = "The maximum number of connections allowed per host for this CosmosClient " +
                                "instance. Those connections will be kept in a pool when idle. Once the pool " +
                                "is exhausted, any operation requiring a connection will block waiting for an " +
                                "available connection.",
                        defaultValue = "100",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "maxConnectionLifeTime",
                        description = "The maximum life time of a pooled connection.  A zero value indicates " +
                                "no limit to the life time.  A pooled connection that has exceeded its life time " +
                                "will be closed and replaced when necessary by a new connection.",
                        defaultValue = "0",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "socketKeepAlive",
                        description = "Sets whether to keep a connection alive through firewalls",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "socketTimeout",
                        description = "The time in milliseconds to attempt a send or receive on a socket " +
                                "before the attempt times out. Default 0 means never to timeout.",
                        defaultValue = "0",
                        possibleParameters = "Any natural integer"),
                @SystemParameter(name = "writeConcern",
                        description = "The write concern to use.",
                        defaultValue = "acknowledged",
                        possibleParameters = {"acknowledged", "w1", "w2", "w3", "unacknowledged", "fsynced",
                                "journaled", "replica_acknowledged", "normal", "safe", "majority", "fsync_safe",
                                "journal_safe", "replicas_safe"}),
                @SystemParameter(name = "readConcern",
                        description = "The level of isolation for the reads from replica sets.",
                        defaultValue = "default",
                        possibleParameters = {"local", "majority", "linearizable"}),
                @SystemParameter(name = "readPreference",
                        description = "Specifies the replica set read preference for the connection.",
                        defaultValue = "primary",
                        possibleParameters = {"primary", "secondary", "secondarypreferred", "primarypreferred",
                                "nearest"}),
                @SystemParameter(name = "localThreshold",
                        description = "The size (in milliseconds) of the latency window for selecting among " +
                                "multiple suitable CosmosDB instances.",
                        defaultValue = "15",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "serverSelectionTimeout",
                        description = "Specifies how long (in milliseconds) to block for server selection " +
                                "before throwing an exception. A value of 0 means that it will timeout immediately " +
                                "if no server is available.  A negative value means to wait indefinitely.",
                        defaultValue = "30000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatSocketTimeout",
                        description = "The socket timeout for connections used for the cluster heartbeat. A value of " +
                                "0 means that it will timeout immediately if no cluster member is available.  " +
                                "A negative value means to wait indefinitely.",
                        defaultValue = "20000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatConnectTimeout",
                        description = "The connect timeout for connections used for the cluster heartbeat. A value " +
                                "of 0 means that it will timeout immediately if no cluster member is available.  " +
                                "A negative value means to wait indefinitely.",
                        defaultValue = "20000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatFrequency",
                        description = "Specify the interval (in milliseconds) between checks, counted from " +
                                "the end of the previous check until the beginning of the next one.",
                        defaultValue = "10000",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "minHeartbeatFrequency",
                        description = "Sets the minimum heartbeat frequency.  In the event that the driver " +
                                "has to frequently re-check a server's availability, it will wait at least this " +
                                "long since the previous check to avoid wasted effort.",
                        defaultValue = "500",
                        possibleParameters = "Any positive integer")
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
    private String collectionId = "Test1";


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

        for (Object[] record : records) {
            Map<String, Object> insertMap = CosmosTableUtils.mapValuesToAttributes(record, this.attributeNames);
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
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        //this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
        //for (int i = 0; i < deleteConditionParameterMaps.size(); i++) {
        for (Map<String, Object> conditionParams : deleteConditionParameterMaps) {
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


        }


        //Uncomment later
        /*try {
            for (Map<String, Object> deleteConditionParameter : deleteConditionParameterMaps) {
                String id = CosmosTableUtils.resolveCondition((CosmosCompiledCondition) compiledCondition, deleteConditionParameter);
                List<Document> toDeleteDocument = documentClient
                        .queryDocuments(getcollectionId().getSelfLink(),
                                "SELECT * FROM root r WHERE r.id='" + id + "'", null)
                        .getQueryIterable().toList();

                if (toDeleteDocument.size() > 0) {
                    documentClient.deleteDocument(toDeleteDocument.get(0).getSelfLink(), null);
                }
                //Document toDeleteDocument = CosmosTableUtils.resolveCondition((CosmosCompiledCondition) compiledCondition, deleteConditionParameter);
                //Document todeleteDocument = CosmosTableUtils.resolveCondition(todeleteDocument, (CosmosCompiledCondition) compiledCondition,
                //deleteConditionParameter, 0);
            }
        } catch (DocumentClientException e) {
            throw new CosmosTableException("Error while deleting records from collection : " + collectionId, e);
        }*/

    }

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
        CosmosExpressionVisitor visitor = new CosmosExpressionVisitor();
        expressionBuilder.build(visitor);
        return new CosmosCompiledCondition(visitor.getCompiledCondition(), visitor.getPlaceholders());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        CosmosSetExpressionVisitor visitor = new CosmosSetExpressionVisitor();
        expressionBuilder.build(visitor);
        return new CosmosCompiledCondition(visitor.getCompiledCondition(), visitor.getPlaceholders());
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


    /*

    private void populateStatement(Object[] record, PreparedStatement stmt) throws ConnectionUnavailableException {
        Attribute attribute = null;
        try {
            for (int i = 0; i < this.attributes.size(); i++) {
                attribute = this.attributes.get(i);
                Object value = record[i];
                if (value != null || attribute.getType() == Attribute.Type.STRING) {
                    CosmosTableUtils.populateStatementWithSingleElement(stmt, i + 1, attribute.getType(), value);
                } else {
                    throw new CosmosTableException("Cannot Execute Insert/Update: null value detected for " +
                            "attribute '" + attribute.getName() + "'");
                }
            }
        } catch (SQLException e) {
            try {
                if (!stmt.getConnection().isValid(0)) {
                    throw new ConnectionUnavailableException("Connection is closed. Could not execute Insert/Update " +
                            " for store: '" + collectionId + "'", e);
                } else {
                    throw new CosmosTableException("Dropping event since value for attribute name " +
                            attribute.getName() + " cannot be set for store: " + collectionId, e);
                }
            } catch (SQLException e1) {
                throw new CosmosTableException("Could not execute Insert/Update for store: '" + collectionId + "'", e1);
            }
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
