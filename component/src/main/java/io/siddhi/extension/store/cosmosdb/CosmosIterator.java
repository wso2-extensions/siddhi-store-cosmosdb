/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
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

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.extension.store.cosmosdb.exception.CosmosTableException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class representing a RecordIterator which is responsible for processing CosmosDB Event Table find() operations in a
 * streaming fashion.
 */
public class CosmosIterator implements RecordIterator<Object[]> {

    private boolean preFetched;
    private Object[] nextValue;
    private Object[] attributes;
    private List<Document> documents;
    private static DocumentClient documentClient;


    public CosmosIterator(List<Document> documentList, Object[] attributes) {
        this.attributes = attributes;
        this.documents = documentList;
    }

    @Override
    public boolean hasNext() {
        if (!this.preFetched) {
            this.nextValue = this.next();
            this.preFetched = true;
        }
        return nextValue != null;
    }

    @Override
    public Object[] next() {
        if (this.preFetched) {
            this.preFetched = false;
            Object[] result = this.nextValue;
            this.nextValue = null;
            return result;
        }
        try {
            if (!this.documents.isEmpty()) {
                return this.extractRecord(this.documents);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new CosmosTableException("Error retrieving records from table '"
                    + e.getMessage(), e);
        }
    }

    /**
     * Method which is used for extracting record values (in the form of an Object array) from an SQL {@link ResultSet},
     * according to the table's field type order.
     *
     * @param documentList the {@link List} from which the values should be retrieved.
     * @return an array of extracted values, all cast to {@link Object} type for portability.
     * @throws SQLException if there are errors in extracting the values from the {@link List} instance according
     *                      to the table definition
     */
    private Object[] extractRecord(List<Document> documentList) throws SQLException {
        List<Object> result = new ArrayList<>();
        //for (Document document : documentList) {
        for (int i = 0; i < documentList.size(); i++) {
            Document document = documentList.get(i);

            for (Object attributeName : attributes) {
                //Object attributeValue = document.getObject("'"+attributeName.toString()+"'");
                //Object attributeValue = document.propertyBag.get(attributeName.getName());
                //Object attributeValue = attributeName;
                Object attributeValue = document;

                /*try {
                    attributeValue = documentClient.readDocument(document.getString(String.valueOf(attributeName)), null);
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                }*/

                /*SqlQuerySpec query = new SqlQuerySpec();
                query.setQueryText("SELECT VALUE FROM " + document + " WHERE " + attributeName.getName());
                FeedOptions options = new FeedOptions();
                options.setEnableScanInQuery(true);*/
                result.add(attributeValue);
            }
            documentList.remove(document);
            //return result.toArray();
        }
        return result.toArray();
    }

    @Override
    public void remove() {
        //Do nothing. This is a read-only iterator.
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected void finalize() throws Throwable {
        //In the unlikely case this iterator does not go to the end, we have to make sure the connection is cleaned up.
        super.finalize();
    }

}


