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

import com.microsoft.azure.documentdb.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class CosmosTableTestUtils {

    private static final Log log = LogFactory.getLog(CosmosTableTestUtils.class);
    private static final String uri = "https://a6cec7ca-0ee0-4-231-b9ee.documents.azure.com:443/";
    private static final String key =
            "pNXzmZ7T6Fxw7di1aeOML9USfUEGMwyWjUVwZw8mYemeu3ro7UkkqxOsrjpZk8g7j5PS2YejpRYf8ONWwgr2GA==";
    private static final String containerName = "admin";
    private static final String collectionName = "fooTable";
    //private static String databaseName = "admin";

    private CosmosTableTestUtils() {
    }

    public static String resolveBaseUri() {
        return uri;
    }

    public static String resolveMasterKey() {
        return key;
    }

    public static String resolveContainer() {
        return containerName;
    }


    private static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    public static void dropCollection(String uri, String key, String collectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            documentClient.deleteCollection(collectionName, null);
        } catch (DocumentClientException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
        }
    }

    public static long getDocumentsCount(String uri, String key, String collectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            SqlQuerySpec query = new SqlQuerySpec();
            //query.setQueryText("SELECT VALUE COUNT(1) FROM " + collectionName);
            query.setQueryText("SELECT * FROM " + collectionName);
            FeedOptions options = new FeedOptions();
            options.setEnableScanInQuery(true);
            List<Document> documentList = documentClient.queryDocuments(collectionName,
                    query, options).getQueryIterable().toList();
            return documentList.size();
        }
    }

    public static boolean doesCollectionExists(String uri, String key, String containerName,
                                               String customCollectionName) {
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            List<DocumentCollection> collectionList = documentClient.queryCollections(containerName,
                    "SELECT * FROM root r WHERE r.id='" + customCollectionName + "'",
                    null).getQueryIterable().toList();

            return collectionList.size() > 0;
        } catch (Exception e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
            throw e;
        }
    }

    public static void createCollection(String uri, String key, String containerName, String collectionName) {
        dropCollection(uri, key, "FooTable");
        try (DocumentClient documentClient = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {
            try {
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.setId(collectionName);
                documentClient.createCollection(containerName, collectionDefinition, null);
            } catch (DocumentClientException e) {
                log.error("Failed to create the collection", e);
            }
        }
    }
}


