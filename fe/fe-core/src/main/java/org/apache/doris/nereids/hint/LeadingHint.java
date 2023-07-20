// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.hint;

import com.google.common.collect.Maps;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.rewrite.CollectJoinConstraint;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class LeadingHint extends Hint {
    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final List<String> tablelist = new ArrayList<>();
    private final List<Integer> levellist = new ArrayList<>();

    private final Map<String, LogicalPlan> tableNameToScanMap = Maps.newLinkedHashMap();

    private final List<Pair<Long, Expression>> filters = new ArrayList<>();

//    private final List<CollectJoinConstraint>

    public LeadingHint(String hintName) {
        super(hintName);
    }

    /**
     * Leading hint data structure before using
     * @param hintName Leading
     * @param parameters table name mixed with left and right brace
     */
    public LeadingHint(String hintName, List<String> parameters) {
        super(hintName);
        int level = 0;
        for (String parameter : parameters) {
            if (parameter.equals("{")) {
                ++level;
            } else if (parameter.equals("}")) {
                level--;
            } else {
                tablelist.add(parameter);
                levellist.add(level);
            }
        }
    }

    public List<String> getTablelist() {
        return tablelist;
    }

    public List<Integer> getLevellist() {
        return levellist;
    }

    public Map<String, LogicalPlan> getTableNameToScanMap() {
        return tableNameToScanMap;
    }

    public List<Pair<Long,Expression>> getFilters() {
        return filters;
    }

    //    @Override
//    public String toString() {
//        String leadingString = parameters
//            .stream()
//            .collect(Collectors.joining(", "));
//        return super.getHintName() + "(" + leadingString + ")";
//    }
}
