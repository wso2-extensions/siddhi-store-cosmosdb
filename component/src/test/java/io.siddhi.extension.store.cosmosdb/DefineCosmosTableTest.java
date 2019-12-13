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
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DefineCosmosTableTest {

    private static final Logger log = Logger.getLogger(DefineCosmosTableTest.class);

    private static String uri = CosmosTableTestUtils.resolveBaseUri();
    private static final String key = CosmosTableTestUtils.resolveMasterKey();
    private static final String database = CosmosTableTestUtils.resolveDatabase();

    @BeforeClass
    public void init() {
        log.info("== Cosmos Table DEFINITION tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Cosmos Table DEFINITION tests completed ==");
    }

    @Test
    public void cosmosTableDefinitionTest1() {
        log.info("cosmosTableDefinitionTest1 - " +
                "Defining a CosmosDB event table with a non existing collection.");

        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        String databaseLink = String.format("/dbs/%s", database);
        boolean doesCollectionExists = CosmosTableTestUtils.doesCollectionExists(uri, key, databaseLink,
                "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void cosmosTableDefinitionTest2() {
        log.info("cosmosTableDefinitionTest2 - " +
                "Defining a CosmosDB event table with an existing collection");

        CosmosTableTestUtils.createCollection(uri, key, database, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FunTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        String databaseLink = String.format("/dbs/%s", database);
        boolean doesCollectionExists = CosmosTableTestUtils.doesCollectionExists(uri, key, databaseLink,
                "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest3() {
        log.info("cosmosTableDefinitionTest3 - " +
                "Defining a CosmosDB table without having a cosmosdb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest4() {
        log.info("cosmosTableDefinitionTest4 - " +
                "Defining a CosmosDB table without defining a value for cosmosdb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb', uri='', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest5() {
        log.info("cosmosTableDefinitionTest5 - " +
                "Defining a CosmosDBS table with an invalid value for cosmosdb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb', uri='123456', access.key='" + key + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void cosmosTableDefinitionTest6() {
        log.info("cosmosTableDefinitionTest6 - " +
                "Defining a CosmosDB event table with a new collection name");

        String collectionLink = String.format("/dbs/%s/colls/%s", database, "FooTable");
        CosmosTableTestUtils.dropCollection(uri, key, collectionLink);

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='" + database + "', collection.name='newCollection') " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        String databaseLink = String.format("/dbs/%s", database);
        boolean doesCollectionExists = CosmosTableTestUtils.doesCollectionExists(uri, key, databaseLink,
                "newCollection");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest7() {
        log.info("cosmosTableDefinitionTest7 - " +
                "Defining a CosmosDB table without having a cosmosdb access.key field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest8() {
        log.info("cosmosTableDefinitionTest8 - " +
                "Defining a CosmosDB table without defining a value for cosmosdb access.key field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb', uri='" + uri + "', access.key='', " +
                "database.name='" + database + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest9() {
        log.info("cosmosTableDefinitionTest9 - " +
                "Defining a CosmosDB table without having a database name field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cosmosTableDefinitionTest10() {
        log.info("cosmosTableDefinitionTest10 - " +
                "Defining a CosmosDB table without defining a value for database name field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'cosmosdb' , uri='" + uri + "', access.key='" + key + "', " +
                "database.name='')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
