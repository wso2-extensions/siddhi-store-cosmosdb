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
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class JoinCosmosTableTest {

    private static final Logger log = Logger.getLogger(JoinCosmosTableTest.class);

    private static String uri = CosmosTableTestUtils.resolveBaseUri();
    private static String key = CosmosTableTestUtils.resolveMasterKey();
    private static String database = CosmosTableTestUtils.resolveDatabase();
    private AtomicInteger eventCount = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        log.info("== Cosmos Table JOIN tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Cosmos Table JOIN tests completed ==");
    }

    @BeforeMethod
    public void testInit() {
        eventCount.set(0);
    }

    @Test
    public void testCosmosTableJoinQuery1() throws InterruptedException {
        log.info("testCosmosTableJoinQuery1 -" +
                "Read events from a CosmosDB collection successfully");

        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2_check", "WSO2", 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2_check", "IBM", 10}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 5.6f, 100L});
        stockStream.send(new Object[]{"IBM", 7.6f, 10L});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testCosmosTableJoinQuery2() {
        log.info("testCosmosTableJoinQuery - Read events from a non existing CosmosDB collection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable123 " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testCosmosTableJoinQuery3() {
        log.info("testCosmosTableJoinQuery3 - " +
                "Read events from a CosmosDB collection by sending through non existing stream");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testCosmosTableJoinQuery4() throws InterruptedException {
        log.info("testCosmosTableJoinQuery4 - " +
                "Read events from a CosmosDB collection for less attributes than total attribute list");

        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2_check", "WSO2"}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2_check", "IBM"}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 5.6f, 100L});
        stockStream.send(new Object[]{"IBM", 7.6f, 10L});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testCosmosTableJoinQuery5() {
        log.info("testCosmosTableJoinQuery5 - " +
                "DASC5-919:Read events from a CosmosDB collection for non existing attributes");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123#window.length(1) join FooTable " +
                "select FooStream.hello as checkHello, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testCosmosTableJoinQuery6() throws InterruptedException {
        log.info("testCosmosTableJoinQuery6");
        //Object reads

        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, input Object); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, input Object);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.input as input  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        if (eventCount.intValue() == 1) {
                            String input = "{symbol=IBM}";
                            Assert.assertEquals(new Object[]{"WSO2_check", "WSO2", input}, event.getData());
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        String input = "{symbol=IBM}";
        stockStream.send(new Object[]{"WSO2", 5.6f, input});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }
}
