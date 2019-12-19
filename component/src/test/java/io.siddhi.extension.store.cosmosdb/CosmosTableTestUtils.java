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

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class CosmosTableTestUtils {

    private static final Log log = LogFactory.getLog(CosmosTableTestUtils.class);
    private static final String uri = "https://tikiri.documents.azure.com:443/";
    private static final String key =
            "R2dfUSPCbWCElaOgCfC8QyaUPd3fQyRr9bnFFlne75KFv8acRvuI2HS3NYpShFiGxyGVqlerf7FkT3OdpSC7tQ==";
    private static final String databaseName = "Production";

    private CosmosTableTestUtils() {
    }

    public static String resolveBaseUri() {
        return uri;
    }

    public static String resolveMasterKey() {
        return key;
    }

    public static String resolveDatabase() {
        return databaseName;
    }


    public static void dropCollection(String uri, String key, String collectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            documentClient.deleteCollection(collectionName, null);
        } catch (DocumentClientException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
        }
    }

    public static long getDocumentsCount(String uri, String key, String collectionName, String collectionLink) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            SqlQuerySpec query = new SqlQuerySpec();
            query.setQueryText("SELECT * FROM " + collectionName);
            FeedOptions options = new FeedOptions();
            options.setEnableScanInQuery(true);
            List<Document> documentList = documentClient.queryDocuments(collectionLink,
                    query, options).getQueryIterable().toList();
            return documentList.size();
        } catch (Exception e) {
            log.debug("Getting document count failed due to" + e.getMessage(), e);
            throw e;
        }
    }

    public static boolean doesCollectionExists(String uri, String key, String databaseName,
                                               String customCollectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            List<DocumentCollection> collectionList = documentClient.queryCollections(databaseName,
                    "SELECT * FROM root r WHERE r.id='" + customCollectionName + "'",
                    null).getQueryIterable().toList();

            return collectionList.size() > 0;
        } catch (Exception e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
            throw e;
        }
    }

    public static void createCollection(String uri, String key, String databaseName, String collectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
            dropCollection(uri, key, collectionLink);
            try {
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.setId(collectionName);
                String databaseLink = String.format("/dbs/%s", databaseName);
                documentClient.createCollection(databaseLink, collectionDefinition, null);
            } catch (DocumentClientException e) {
                log.error("Failed to create the collection", e);
            }
        }
    }

    public static Document getDocument(String uri, String accessKey, String collectionLink, String collectionName,
                                       String value) {
        try (DocumentClient documentClient = new DocumentClient(uri, accessKey, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            SqlQuerySpec query = new SqlQuerySpec();
            query.setQueryText("SELECT * FROM " + collectionName + " WHERE " + value);
            FeedOptions options = new FeedOptions();
            options.setEnableScanInQuery(true);
            List<Document> documentList = documentClient.queryDocuments(collectionLink,
                    query, options).getQueryIterable().toList();
            Document document = documentList.get(0);
            return document;
        } catch (Exception e) {
            log.error("Failed to get the document", e);
            throw e;
        }
    }
}


