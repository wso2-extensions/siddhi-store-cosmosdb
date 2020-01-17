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

public class ConditionVisitorTest {

    private final Log log = LogFactory.getLog(ConditionVisitorTest.class);
    private static String uri = CosmosTableTestUtils.resolveBaseUri();
    private static final String key = CosmosTableTestUtils.resolveMasterKey();
    private static final String database = CosmosTableTestUtils.resolveDatabase();

    @BeforeClass
    public void init() {
        log.info("== CosmosDB Collection Condition Visitor tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== CosmosDB Collection IN tests completed ==");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void conditionBuilderTest1() {
        log.info("conditionBuilderTest1 - Test delete on condition when there's a non-existing attribute as right" +
                " operand. ");
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
                "   on 'IBM' == amount  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void conditionBuilderTest2() {
        log.info("conditionBuilderTest2 - Test delete on condition when there's a non-existing attribute as left" +
                " operand. ");
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
                "   on amount == 'IBM'  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void conditionBuilderTest3() throws InterruptedException {
        log.info("conditionBuilderTest3 - Delete an event of a CosmosDB table successfully by having event table" +
                " attribute as left operand. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.symbol==symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest4() throws InterruptedException {
        log.info("conditionBuilderTest4 - Delete an event of a CosmosDB table successfully by having event table" +
                " attribute as right operand. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on symbol == FooTable.symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest5() throws InterruptedException {
        log.info("conditionBuilderTest5 - Delete an event of a CosmosDB table successfully when having a " +
                "constant as left operand. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on 'IBM' == FooTable.symbol  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest6() throws InterruptedException {
        log.info("conditionBuilderTest6 - Delete an event of a CosmosDB table successfully when having a constant" +
                " as right operand. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.symbol == 'IBM'  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest7() throws InterruptedException {
        log.info("conditionBuilderTest7 - Test delete on condition when there's a not equals operator. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.symbol != symbol  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest8() throws InterruptedException {
        log.info("conditionBuilderTest8 - Test delete on condition when there's a greater than operator. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.price > price  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest9() throws InterruptedException {
        log.info("conditionBuilderTest9 - Test delete on condition when there's a greater than or equal operator. ");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.price >= 57.6F  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest10() throws InterruptedException {
        log.info("conditionBuilderTest10 - Test delete on condition when there's a less than operator.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.price < price  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest11() throws InterruptedException {
        log.info("conditionBuilderTest11 - Test delete on condition when there's a less than or equal operator.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.price <= price  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest12() throws InterruptedException {
        log.info("conditionBuilderTest12 - Test delete on condition when there's a logical AND operator.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.symbol == symbol AND FooTable.price <= price  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 55.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest13() throws InterruptedException {
        log.info("conditionBuilderTest13 - Test delete on condition when there's a logical OR operator.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on FooTable.symbol == symbol OR FooTable.price <= price  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 55.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 0, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest14() throws InterruptedException {
        log.info("conditionBuilderTest14 - Test delete on condition when there's a logical NOT operator.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on NOT (FooTable.symbol == symbol);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 55.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void conditionBuilderTest15() throws InterruptedException {
        log.info("conditionBuilderTest15 - Test delete on condition when there's an 'in' condition.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on (FooTable.symbol) in symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 55.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
    }

    @Test
    public void conditionBuilderTest16() throws InterruptedException {
        log.info("conditionBuilderTest16 - Test delete on condition when there's a NULL condition.");
        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
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
                "   on NOT (FooTable.symbol is NULL);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 55.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        siddhiAppRuntime.shutdown();
        long totalDocumentsInCollection = CosmosTableTestUtils.getDocumentsCount(uri, key, "FooTable",
                collectionLink);
        Assert.assertEquals(totalDocumentsInCollection, 0, "Deletion failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void conditionBuilderTest17() {
        log.info("conditionBuilderTest17 - Test delete on condition when there's an invalid operation. ");
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
                "   on DateOf(FooTable.price) < 67;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
