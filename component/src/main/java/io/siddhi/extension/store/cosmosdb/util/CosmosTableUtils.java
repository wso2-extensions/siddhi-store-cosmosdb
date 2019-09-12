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
package io.siddhi.extension.store.cosmosdb.util;

import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.cosmosdb.exception.CosmosTableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.siddhi.extension.store.cosmosdb.util.CosmosTableConstants.VARIABLE_CARBON_HOME;

/**
 * Class which holds the utility methods which are used by various units in the CosmosDB Event Table implementation.
 */
public class CosmosTableUtils {
    private static final Log log = LogFactory.getLog(CosmosTableUtils.class);

    private CosmosTableUtils() {
        //Prevent Initialization.
    }

    /**
     * Utility method which can be used to check if the given primary key is valid i.e. non empty
     * and is made up of attributes and return an index model when PrimaryKey is valid.
     *
     * @param primaryKey     the PrimaryKey annotation which contains the primary key attributes.
     * @param attributeNames List containing names of the attributes.
     * @return List of String with primary key attributes.
     */
   /* public static IndexModel extractPrimaryKey(Annotation primaryKey, List<String> attributeNames) {
        if (primaryKey == null) {
            return null;
        }
        Document primaryKeyIndex = new Document();
        primaryKey.getElements().forEach(
                element -> {
                    if (!isEmpty(element.getValue()) && attributeNames.contains(element.getValue())) {
                        primaryKeyIndex.append(element.getValue(), 1);
                    } else {
                        throw new SiddhiAppCreationException("Annotation '" + primaryKey.getName() + "' contains " +
                                "value '" + element.getValue() + "' which is not present in the attributes of the " +
                                "Event Table.");
                    }
                }
        );
        return new IndexModel(primaryKeyIndex, new IndexOptions().unique(true));
    }*/

    /**
     * Utility method which can be used to check if the given Indices are valid  and return List of
     * CosmosDB Index Models when valid.
     *
     * @param indices        the IndexBy annotation which contains the indices definitions.
     * @param attributeNames List containing names of the attributes.
     * @return List of IndexModel.
     */
   /* public static List<IndexModel> extractIndexModels(Annotation indices, List<String> attributeNames) {
        if (indices == null) {
            return new ArrayList<>();
        }
        Pattern indexBy = Pattern.compile(CosmosTableConstants.REG_INDEX_BY);
        return indices.getElements().stream().map(index -> {
            Matcher matcher = indexBy.matcher(index.getValue());
            if (matcher.matches() && attributeNames.contains(matcher.group(1))) {
                if (matcher.groupCount() == 4) {
                    return createIndexModel(
                            matcher.group(1), Integer.parseInt(matcher.group(2)), matcher.group(3).trim());
                } else {
                    if (matcher.groupCount() == 3) {
                        if (matcher.group(3) == null) {
                            return createIndexModel(
                                    matcher.group(1), Integer.parseInt(matcher.group(2).trim()), null);
                        } else {
                            return createIndexModel(matcher.group(1), 1, matcher.group(3).trim());
                        }
                    } else {
                        return createIndexModel(matcher.group(1), 1, null);
                    }
                }
            } else {
                throw new SiddhiAppCreationException("Annotation '" + indices.getName() + "' contains illegal " +
                        "value : '" + index.getValue() + "'. Please check your query and try again.");
            }
        }).collect(Collectors.toList());
    }
*/
    /**
     * Utility method which can be used to create an IndexModel.
     *
     * @param fieldName   the attribute on which the index is to be created.
     * @param sortOrder   the sort order of the index to be created.
     * @param indexOption json string of the options of the index to be created.
     * @return IndexModel.
     */
    /*private static IndexModel createIndexModel(String fieldName, Integer sortOrder, String indexOption) {
        Document indexDocument = new Document(fieldName, sortOrder);
        if (indexOption == null) {
            return new IndexModel(indexDocument);
        } else {
            IndexOptions indexOptions = new IndexOptions();
            Document indexOptionDocument;
            try {
                indexOptionDocument = Document.parse(indexOption);
                for (Map.Entry<String, Object> indexEntry : indexOptionDocument.entrySet()) {
                    Object value = indexEntry.getValue();
                    switch (indexEntry.getKey()) {
                        case "unique":
                            indexOptions.unique(Boolean.parseBoolean(value.toString()));
                            break;
                        case "background":
                            indexOptions.background(Boolean.parseBoolean(value.toString()));
                            break;
                        case "name":
                            indexOptions.name(value.toString());
                            break;
                        case "sparse":
                            indexOptions.sparse(Boolean.parseBoolean(value.toString()));
                            break;
                        case "expireAfterSeconds":
                            indexOptions.expireAfter(Long.parseLong(value.toString()), TimeUnit.SECONDS);
                            break;
                        case "version":
                            indexOptions.version(Integer.parseInt(value.toString()));
                            break;
                        case "weights":
                            indexOptions.weights((Bson) value);
                            break;
                        case "languageOverride":
                            indexOptions.languageOverride(value.toString());
                            break;
                        case "defaultLanguage":
                            indexOptions.defaultLanguage(value.toString());
                            break;
                        case "textVersion":
                            indexOptions.textVersion(Integer.parseInt(value.toString()));
                            break;
                        case "sphereVersion":
                            indexOptions.sphereVersion(Integer.parseInt(value.toString()));
                            break;
                        case "bits":
                            indexOptions.bits(Integer.parseInt(value.toString()));
                            break;
                        case "min":
                            indexOptions.min(Double.parseDouble(value.toString()));
                            break;
                        case "max":
                            indexOptions.max(Double.parseDouble(value.toString()));
                            break;
                        case "bucketSize":
                            indexOptions.bucketSize(Double.parseDouble(value.toString()));
                            break;
                        case "partialFilterExpression":
                            indexOptions.partialFilterExpression((Bson) value);
                            break;
                        case "collation":
                            DBObject collationOptions = (DBObject) value;
                            Collation.Builder builder = Collation.builder();
                            for (String collationKey : collationOptions.keySet()) {
                                String collationObj = value.toString();
                                switch (collationKey) {
                                    case "locale":
                                        builder.locale(collationObj);
                                        break;
                                    case "caseLevel":
                                        builder.caseLevel(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "caseFirst":
                                        builder.collationCaseFirst(CollationCaseFirst.fromString(collationObj));
                                        break;
                                    case "strength":
                                        builder.collationStrength(CollationStrength.valueOf(collationObj));
                                        break;
                                    case "numericOrdering":
                                        builder.numericOrdering(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "normalization":
                                        builder.normalization(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "backwards":
                                        builder.backwards(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "alternate":
                                        builder.collationAlternate(CollationAlternate.fromString(collationObj));
                                        break;
                                    case "maxVariable":
                                        builder.collationMaxVariable(CollationMaxVariable.fromString(collationObj));
                                        break;
                                    default:
                                        log.warn("Annotation 'IndexBy' for the field '" + fieldName + "' contains " +
                                                "unknown 'Collation' Option key : '" + collationKey + "'. Please " +
                                                "check your query and try again.");
                                        break;
                                }
                            }
                            if (builder.build().getLocale() != null) {
                                indexOptions.collation(builder.build());
                            } else {
                                throw new CosmosTableException("Annotation 'IndexBy' for the field '" + fieldName + "'" +
                                        " do not contain option for locale. Please check your query and try again.");
                            }
                            break;
                        case "storageEngine":
                            indexOptions.storageEngine((Bson) value);
                            break;
                        default:
                            log.warn("Annotation 'IndexBy' for the field '" + fieldName + "' contains unknown option " +
                                    "key : '" + indexEntry.getKey() + "'. Please check your query and try again.");
                            break;
                    }
                }
            } catch (JsonParseException | NumberFormatException e) {
                throw new CosmosTableException("Annotation 'IndexBy' for the field '" + fieldName + "' contains " +
                        "illegal value(s) for index option. Please check your query and try again.", e);
            }
            return new IndexModel(indexDocument, indexOptions);
        }
    }
*/
    /**
     * Utility method which can be used to resolve the condition with the runtime values and return a Document
     * describing the filter.
     *
     * @param compiledCondition     the compiled condition which was built during compile time and now is being provided
     *                              by the Siddhi runtime.
     * @param conditionParameterMap the map which contains the runtime value(s) for the condition.
     * @return Document.
     */
/*    public static Document resolveCondition(CosmosCompiledCondition compiledCondition,
                                            Map<String, Object> conditionParameterMap) {
        Map<String, Object> parameters = compiledCondition.getPlaceholders();
        String compiledQuery = compiledCondition.getCompiledQuery();
        if (compiledQuery.equalsIgnoreCase("true")) {
            return new Document();
        }
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Object parameter = entry.getValue();
            Attribute variable = (Attribute) parameter;
            if (variable.getType().equals(Attribute.Type.STRING)) {
                compiledQuery = compiledQuery.replaceAll(entry.getKey(), "\"" +
                        conditionParameterMap.get(variable.getName()).toString() + "\"");
            } else {
                compiledQuery = compiledQuery.replaceAll(entry.getKey(),
                        conditionParameterMap.get(variable.getName()).toString());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("The final compiled query : '" + compiledQuery + "'");
        }
        return Document.parse(compiledQuery);
    }*/

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }


    /**
     * Utility method tp map the values to the respective attributes before database writes.
     *
     * @param record         Object array of the runtime values.
     * @param attributeNames List containing names of the attributes.
     * @return Document
     */
    public static Map<String, Object> mapValuesToAttributes(Object[] record, List<String> attributeNames) {
        Map<String, Object> attributesValuesMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValuesMap.put(attributeNames.get(i), record[i]);
        }
        return attributesValuesMap;
    }

    /**
     * Utility method which can be used to check if the existing indices contain the expected indices
     * defined by the annotation 'PrimaryKey' and 'IndexBy' and log a warning when indices differs.
     *
     * @param existingIndices List of indices that the collection contains.
     * @param expectedIndices List of indices that are defined by the annotations.
     */
   /* public static void checkExistingIndices(List<IndexModel> expectedIndices, CosmosCursor<Document> existingIndices) {
        Map<String, Object> indexOptionsMap = new HashMap<>();
        List<Document> expectedIndexDocuments = expectedIndices.stream().map(expectedIndex -> {
            IndexOptions expectedIndexOptions = expectedIndex.getOptions();
            indexOptionsMap.put("key", expectedIndex.getKeys());
            // Default value for name of the index
            if (expectedIndexOptions.getName() == null) {
                StringBuilder indexName = new StringBuilder();
                ((Document) expectedIndex.getKeys()).forEach((key, value) ->
                        indexName.append("_").append(key).append("_").append(value));
                indexName.deleteCharAt(0);
                indexOptionsMap.put("name", indexName.toString());
            } else {
                indexOptionsMap.put("name", expectedIndexOptions.getName());
            }
            // Default value for the version
            if (expectedIndexOptions.getVersion() == null) {
                indexOptionsMap.put("v", 2);
            } else {
                indexOptionsMap.put("v", expectedIndexOptions.getVersion());
            }
            indexOptionsMap.put("unique", expectedIndexOptions.isUnique());
            indexOptionsMap.put("background", expectedIndexOptions.isBackground());
            indexOptionsMap.put("sparse", expectedIndexOptions.isSparse());
            indexOptionsMap.put("expireAfterSeconds", expectedIndexOptions.getExpireAfter(TimeUnit.SECONDS));
            indexOptionsMap.put("weights", expectedIndexOptions.getWeights());
            indexOptionsMap.put("languageOverride", expectedIndexOptions.getLanguageOverride());
            indexOptionsMap.put("defaultLanguage", expectedIndexOptions.getDefaultLanguage());
            indexOptionsMap.put("textVersion", expectedIndexOptions.getTextVersion());
            indexOptionsMap.put("sphereVersion", expectedIndexOptions.getSphereVersion());
            indexOptionsMap.put("textVersion", expectedIndexOptions.getTextVersion());
            indexOptionsMap.put("bits", expectedIndexOptions.getBits());
            indexOptionsMap.put("min", expectedIndexOptions.getMin());
            indexOptionsMap.put("max", expectedIndexOptions.getMax());
            indexOptionsMap.put("bucketSize", expectedIndexOptions.getBucketSize());
            indexOptionsMap.put("partialFilterExpression", expectedIndexOptions.getPartialFilterExpression());
            indexOptionsMap.put("collation", expectedIndexOptions.getCollation());
            indexOptionsMap.put("storageEngine", expectedIndexOptions.getStorageEngine());

            //Remove if Default Values - these would not be in the existingIndexDocument.
            indexOptionsMap.values().removeIf(Objects::isNull);
            indexOptionsMap.remove("unique", false);
            indexOptionsMap.remove("background", false);
            indexOptionsMap.remove("sparse", false);

            return new Document(indexOptionsMap);
        }).collect(Collectors.toList());

        List<Document> existingIndexDocuments = new ArrayList<>();
        existingIndices.forEachRemaining(existingIndex -> {
            existingIndex.remove("ns");
            existingIndexDocuments.add(existingIndex);
        });

        if (!existingIndexDocuments.containsAll(expectedIndexDocuments)) {
            log.warn("Existing indices differs from the expected indices defined by the Annotations 'PrimaryKey' " +
                    "and 'IndexBy'.\nExisting Indices '" + existingIndexDocuments.toString() + "'.\n" +
                    "Expected Indices '" + expectedIndexDocuments.toString() + "'");
        }
    }
*/
    /**
     * Utility method which can be used to create CosmosClientOptionsBuilder from values defined in the
     * deployment yaml file.
     *
     * //@param storeAnnotation the source annotation which contains the needed parameters.
     * //@param configReader    {@link ConfigReader} Configuration Reader
     * @return CosmosClientOptions.Builder
     */
   /* public static CosmosClientOptions.Builder extractCosmosClientOptionsBuilder
    (Annotation storeAnnotation, ConfigReader configReader) {

        CosmosClientOptions.Builder cosmosClientOptionsBuilder = CosmosClientOptions.builder();
        try {
            cosmosClientOptionsBuilder.connectionsPerHost(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.CONNECTIONS_PER_HOST, "100")));
            cosmosClientOptionsBuilder.connectTimeout(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.CONNECT_TIMEOUT, "10000")));
            cosmosClientOptionsBuilder.heartbeatConnectTimeout(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.HEARTBEAT_CONNECT_TIMEOUT, "20000")));
            cosmosClientOptionsBuilder.heartbeatSocketTimeout(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.HEARTBEAT_SOCKET_TIMEOUT, "20000")));
            cosmosClientOptionsBuilder.heartbeatFrequency(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.HEARTBEAT_FREQUENCY, "10000")));
            cosmosClientOptionsBuilder.localThreshold(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.LOCAL_THRESHOLD, "15")));
            cosmosClientOptionsBuilder.maxWaitTime(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.MAX_WAIT_TIME, "120000")));
            cosmosClientOptionsBuilder.minConnectionsPerHost(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.MIN_CONNECTIONS_PER_HOST, "0")));
            cosmosClientOptionsBuilder.minHeartbeatFrequency(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.MIN_HEARTBEAT_FREQUENCY, "500")));
            cosmosClientOptionsBuilder.serverSelectionTimeout(Integer.parseInt(
                    configReader.readConfig(CosmosTableConstants.SERVER_SELECTION_TIMEOUT, "30000")));
            cosmosClientOptionsBuilder.socketTimeout(
                    Integer.parseInt(configReader.readConfig(CosmosTableConstants.SOCKET_TIMEOUT, "0")));
            cosmosClientOptionsBuilder.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(
                    configReader.readConfig(CosmosTableConstants.THREADS_ALLOWED_TO_BLOCK, "5")));
            cosmosClientOptionsBuilder.socketKeepAlive(
                    Boolean.parseBoolean(configReader.readConfig(CosmosTableConstants.SOCKET_KEEP_ALIVE, "false")));
            cosmosClientOptionsBuilder.sslEnabled(
                    Boolean.parseBoolean(configReader.readConfig(CosmosTableConstants.SSL_ENABLED, "false")));
            cosmosClientOptionsBuilder.cursorFinalizerEnabled(Boolean.parseBoolean(
                    configReader.readConfig(CosmosTableConstants.CURSOR_FINALIZER_ENABLED, "true")));
            cosmosClientOptionsBuilder.readPreference(
                    ReadPreference.valueOf(configReader.readConfig(CosmosTableConstants.READ_PREFERENCE, "primary")));
            cosmosClientOptionsBuilder.writeConcern(
                    WriteConcern.valueOf(configReader.readConfig(CosmosTableConstants.WRITE_CONCERN, "acknowledged")));

            String readConcern = configReader.readConfig(CosmosTableConstants.READ_CONCERN, "DEFAULT");
            if (!readConcern.matches("DEFAULT")) {
                cosmosClientOptionsBuilder.readConcern(new ReadConcern(
                        ReadConcernLevel.fromString(readConcern)));
            }

            int maxConnectionIdleTime = Integer.parseInt(
                    configReader.readConfig(CosmosTableConstants.MAX_CONNECTION_IDLE_TIME, "0"));
            if (maxConnectionIdleTime != 0) {
                cosmosClientOptionsBuilder.maxConnectionIdleTime(maxConnectionIdleTime);
            }

            int maxConnectionLifeTime = Integer.parseInt(
                    configReader.readConfig(CosmosTableConstants.MAX_CONNECTION_LIFE_TIME, "0"));
            if (maxConnectionIdleTime != 0) {
                cosmosClientOptionsBuilder.maxConnectionLifeTime(maxConnectionLifeTime);
            }

            String requiredReplicaSetName = configReader.readConfig(CosmosTableConstants.REQUIRED_REPLICA_SET_NAME, "");
            if (!requiredReplicaSetName.equals("")) {
                cosmosClientOptionsBuilder.requiredReplicaSetName(requiredReplicaSetName);
            }

            String applicationName = configReader.readConfig(CosmosTableConstants.APPLICATION_NAME, "");
            if (!applicationName.equals("")) {
                cosmosClientOptionsBuilder.applicationName(applicationName);
            }

            String secureConnectionEnabled = storeAnnotation.getElement(
                    CosmosTableConstants.ANNOTATION_ELEMENT_SECURE_CONNECTION);
            secureConnectionEnabled = secureConnectionEnabled == null ? "false" : secureConnectionEnabled;

            if (secureConnectionEnabled.equalsIgnoreCase("true")) {
                cosmosClientOptionsBuilder.sslEnabled(true);
                String trustStore = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_TRUSTSTORE);
                trustStore = trustStore == null ?
                        configReader.readConfig("trustStore", DEFAULT_TRUST_STORE_FILE) : trustStore;
                trustStore = resolveCarbonHome(trustStore);

                String trustStorePassword =
                        storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_TRUSTSTOREPASS);
                trustStorePassword = trustStorePassword == null ?
                        configReader.readConfig("trustStorePassword", DEFAULT_TRUST_STORE_PASSWORD) :
                        trustStorePassword;

                String keyStore = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_KEYSTORE);
                keyStore = keyStore == null ?
                        configReader.readConfig("keyStore", DEFAULT_KEY_STORE_FILE) : keyStore;
                keyStore = resolveCarbonHome(keyStore);

                String keyStorePassword = storeAnnotation.getElement(CosmosTableConstants.ANNOTATION_ELEMENT_STOREPASS);
                keyStorePassword = keyStorePassword == null ?
                        configReader.readConfig("keyStorePassword", DEFAULT_KEY_STORE_PASSWORD) :
                        keyStorePassword;

                cosmosClientOptionsBuilder.socketFactory(CosmosTableUtils
                        .extractSocketFactory(trustStore, trustStorePassword, keyStore, keyStorePassword));
            }
            return cosmosClientOptionsBuilder;
        } catch (IllegalArgumentException e) {
            throw new CosmosTableException("Values Read from config readers have illegal values : ", e);
        }
    }*/

    private static SocketFactory extractSocketFactory(
            String trustStore, String trustStorePassword, String keyStore, String keyStorePassword) {
        TrustManager[] trustManagers;
        KeyManager[] keyManagers;

        try (InputStream trustStream = new FileInputStream(trustStore)) {
            char[] trustStorePass = trustStorePassword.toCharArray();
            KeyStore trustStoreJKS = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStoreJKS.load(trustStream, trustStorePass);
            TrustManagerFactory trustFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trustStoreJKS);
            trustManagers = trustFactory.getTrustManagers();
        } catch (FileNotFoundException e) {
            throw new CosmosTableException("Trust store file not found for secure connections to cosmosdb. " +
                    "Trust Store file path : '" + trustStore + "'.", e);
        } catch (IOException e) {
            throw new CosmosTableException("I/O Exception in creating trust store for secure connections to cosmosdb. " +
                    "Trust Store file path : '" + trustStore + "'.", e);
        } catch (CertificateException e) {
            throw new CosmosTableException("Certificates in the trust store could not be loaded for secure " +
                    "connections to cosmosdb. Trust Store file path : '" + trustStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new CosmosTableException("The algorithm used to check the integrity of the trust store cannot be " +
                    "found. Trust Store file path : '" + trustStore + "'.", e);
        } catch (KeyStoreException e) {
            throw new CosmosTableException("Exception in creating trust store, no Provider supports aKeyStoreSpi " +
                    "implementation for the specified type. Trust Store file path : '" + trustStore + "'.", e);
        }

        try (InputStream keyStream = new FileInputStream(keyStore)) {
            char[] keyStorePass = keyStorePassword.toCharArray();
            KeyStore keyStoreJKS = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStoreJKS.load(keyStream, keyStorePass);
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStoreJKS, keyStorePass);
            keyManagers = keyManagerFactory.getKeyManagers();
        } catch (FileNotFoundException e) {
            throw new CosmosTableException("Key store file not found for secure connections to cosmosdb. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (IOException e) {
            throw new CosmosTableException("I/O Exception in creating trust store for secure connections to cosmosdb. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (CertificateException e) {
            throw new CosmosTableException("Certificates in the trust store could not be loaded for secure " +
                    "connections to cosmosdb. Key Store file path : '" + keyStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new CosmosTableException("The algorithm used to check the integrity of the trust store cannot be " +
                    "found. Key Store file path : '" + keyStore + "'.", e);
        } catch (KeyStoreException e) {
            throw new CosmosTableException("Exception in creating trust store, no Provider supports aKeyStoreSpi " +
                    "implementation for the specified type. Key Store file path : '" + keyStore + "'.", e);
        } catch (UnrecoverableKeyException e) {
            throw new CosmosTableException("Key in the keystore cannot be recovered. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);
            SSLContext.setDefault(sslContext);
            return sslContext.getSocketFactory();
        } catch (KeyManagementException e) {
            throw new CosmosTableException("Error in validating the key in the key store/ trust store. " +
                    "Trust Store file path : '" + trustStore + "'. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new CosmosTableException(" SSL Algorithm used to create SSL Socket Factory for cosmosdb connections " +
                    "is not found.", e);
        }

    }

    private static String resolveCarbonHome(String filePath) {
        String carbonHome = "";
        if (System.getProperty(VARIABLE_CARBON_HOME) != null) {
            carbonHome = System.getProperty(VARIABLE_CARBON_HOME);
        } else if (System.getenv(VARIABLE_CARBON_HOME) != null) {
            carbonHome = System.getenv(VARIABLE_CARBON_HOME);
        }
        return filePath.replaceAll("\\$\\{carbon.home}", carbonHome);
    }
}

