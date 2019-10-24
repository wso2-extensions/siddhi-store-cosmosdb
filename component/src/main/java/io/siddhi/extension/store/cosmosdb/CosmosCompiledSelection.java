/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 */

package io.siddhi.extension.store.cosmosdb;

import io.siddhi.core.util.collection.operator.CompiledSelection;

/**
 * Implementation class of {@link CompiledSelection} corresponding to the CosmosDB Event Table.
 * Maintains the compiled select, group by, having etc. clauses.
 */
public class CosmosCompiledSelection implements CompiledSelection {

    private CosmosCompiledCondition compiledSelectClause;
    private CosmosCompiledCondition compiledGroupByClause;
    private CosmosCompiledCondition compiledHavingClause;
    private CosmosCompiledCondition compiledOrderByClause;
    private Long limit;
    private Long offset;

    public CosmosCompiledSelection(CosmosCompiledCondition compiledSelectClause,
                                   CosmosCompiledCondition compiledGroupByClause,
                                   CosmosCompiledCondition compiledHavingClause,
                                   CosmosCompiledCondition compiledOrderByClause, Long limit, Long offset) {
        this.compiledSelectClause = compiledSelectClause;
        this.compiledGroupByClause = compiledGroupByClause;
        this.compiledHavingClause = compiledHavingClause;
        this.compiledOrderByClause = compiledOrderByClause;
        this.limit = limit;
        this.offset = offset;
    }

    public CosmosCompiledCondition getCompiledSelectClause() {
        return compiledSelectClause;
    }

    public CosmosCompiledCondition getCompiledGroupByClause() {
        return compiledGroupByClause;
    }

    public CosmosCompiledCondition getCompiledHavingClause() {
        return compiledHavingClause;
    }

    public CosmosCompiledCondition getCompiledOrderByClause() {
        return compiledOrderByClause;
    }

    public Long getLimit() {
        return limit;
    }

    public Long getOffset() {
        return offset;
    }
}


