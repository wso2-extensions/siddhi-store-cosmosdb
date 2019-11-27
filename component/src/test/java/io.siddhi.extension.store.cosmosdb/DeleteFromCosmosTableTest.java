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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DeleteFromCosmosTableTest {

    private static final Log log = LogFactory.getLog(DeleteFromCosmosTableTest.class);

    private static String uri = CosmosTableTestUtils.resolveBaseUri();
    private static final String key = CosmosTableTestUtils.resolveMasterKey();
    private static final String database = CosmosTableTestUtils.resolveDatabase();

    @BeforeClass
    public void init() {
        log.info("== CosmosDB Collection DELETE tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== CosmosDB Collection DELETE tests completed ==");
    }

    @Test
    public void deleteFromCosmosTableTest1() throws InterruptedException {
        log.info("deleteFromCosmosTableTest1 - " +
                "Delete an event of a CosmosDB table successfully");

        String collectionLink = String.format("/dbs/%s/colls/%s", "admin", "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.symbol == symbol) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO52", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 55.6F, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromCosmosTableTest2() {
        log.info("deleteFromCosmosTableTest2 - " +
                "Delete an event from a non existing CosmosDB table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable1234 " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromCosmosTableTest3() {
        log.info("deleteFromCosmosTableTest3 - " +
                "Delete an event from a CosmosDB table by selecting from non existing stream");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream345 " +
                "delete FooTable " +
                "on FooTable.symbol == symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromCosmosTableTest4() {
        log.info("deleteFromCosmosTableTest4 - " +
                "Delete an event from a CosmosDB table based on a non-existing attribute");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.length == length) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void deleteFromCosmosTableTest5() throws InterruptedException {
        log.info("deleteFromCosmosTableTest5 - " +
                "Delete an event from a CosmosDB table based on a non-existing attribute value");

        String collectionLink = String.format("/dbs/%s/colls/%s", "admin", "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.symbol == symbol) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM_v2", 75.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2_v2", 55.6F, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 3, "Deletion failed");
    }
}
