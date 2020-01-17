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

import com.microsoft.azure.documentdb.Document;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class UpdateOrInsertCosmosTableTest {

    private static final Logger log = Logger.getLogger(UpdateOrInsertCosmosTableTest.class);
    private static String uri = CosmosTableTestUtils.resolveBaseUri();
    private static String key = CosmosTableTestUtils.resolveMasterKey();
    private static String database = CosmosTableTestUtils.resolveDatabase();

    @BeforeClass
    public void init() {
        log.info("== Cosmos Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Cosmos Table UPDATE/INSERT tests completed ==");
    }

    @Test
    public void updateOrInsertCosmosTableTest1() throws InterruptedException {
        log.info("updateOrInsertCosmosTableTest1 - Configure siddhi to perform insert/update on CosmosDB document");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        fooStream.send(new Object[]{"GOOG", 10.6, 100});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");
        Document expectedUpdatedDocument = new Document();
        expectedUpdatedDocument.set("symbol", "GOOG");
        expectedUpdatedDocument.set("price", 10.6);
        expectedUpdatedDocument.set("volume", 100);
        Document updatedDocument = CosmosTableTestUtils.getDocument(uri, key, collectionLink, "FooTable",
                "FooTable.symbol='GOOG'");
        Assert.assertEquals(updatedDocument.get("symbol"), expectedUpdatedDocument.get("symbol"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("price"), expectedUpdatedDocument.get("price"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("volume"), expectedUpdatedDocument.get("volume"),
                "Update Failed");
    }

    @Test
    public void updateOrInsertCosmosTableTest2() throws InterruptedException {
        log.info("updateOrInsertCosmosTableTest2 - Configure siddhi to perform insert/update on CosmosDB Document " +
                "when no matching record exist");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        fooStream.send(new Object[]{"GOOG_2", 10.6, 100});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 4, "Update failed");
        Document expectedUpdatedDocument = new Document();
        expectedUpdatedDocument.set("symbol", "GOOG_2");
        expectedUpdatedDocument.set("price", 10.6);
        expectedUpdatedDocument.set("volume", 100);
        Document updatedDocument = CosmosTableTestUtils.getDocument(uri, key, collectionLink, "FooTable",
                "FooTable.symbol='GOOG_2'");
        Assert.assertEquals(updatedDocument.get("symbol"), expectedUpdatedDocument.get("symbol"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("price"), expectedUpdatedDocument.get("price"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("volume"), expectedUpdatedDocument.get("volume"),
                "Update Failed");
    }

    @Test
    public void updateOrInsertCosmosTableTest3() throws InterruptedException {
        log.info("updateOrInsertCosmosTableTest3 - Configure siddhi to perform insert/update when some of " +
                "matching records exist");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6, 100});
        fooStream.send(new Object[]{"GOOG_2", 10.6, 100});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");
        Document expectedUpdatedDocument = new Document();
        expectedUpdatedDocument.set("symbol", "WSO2");
        expectedUpdatedDocument.set("price", 57.6);
        expectedUpdatedDocument.set("volume", 100);
        Document updatedDocument = CosmosTableTestUtils.getDocument(uri, key, collectionLink, "FooTable",
                "FooTable.symbol='WSO2'");
        Assert.assertEquals(updatedDocument.get("symbol"), expectedUpdatedDocument.get("symbol"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("price"), expectedUpdatedDocument.get("price"),
                "Update Failed");
        Assert.assertEquals(updatedDocument.get("volume"), expectedUpdatedDocument.get("volume"),
                "Update Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void updateOrInsertCosmosTableTest4() {
        log.info("updateOrInsertCosmosTableTest4 - Configure siddhi to perform insert/update with a non existing " +
                "stream");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123 " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void updateOrInsertCosmosTableTest5() {
        log.info("updateOrInsertCosmosTableTest5 - Configure siddhi to perform insert/update with an undefined " +
                "CosmosDB Document");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable123 " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void updateOrInsertCosmosTableTest6() {
        log.info("updateOrInsertCosmosTableTest6 - Configure siddhi to perform insert/update on CosmosDB Document " +
                "with a non-existing attribute");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol123 == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void updateOrInsertCosmosTableTest7() {
        log.info("updateOrInsertCosmosTableTest7 - Configure siddhi to perform insert/update on CosmosDB Document " +
                "incorrect siddhi query");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooTable " +
                "update or insert into FooStream " +
                "   on FooTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
