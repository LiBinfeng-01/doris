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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** EliminateAggregateSum
 * with group by:
 * select max(1) from t1 group by c1; -> select 1 from (select c1 from t1 group by c1);
 * without group by:
 * select max(1) from t1; -> select max(1) from (select 1 from t1 limit 1) tmp;
 * */
public class EliminateAggregateConstant extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            Set<AggregateFunction> aggFunctions = agg.getAggregateFunctions();
            // check whether we have aggregate(constant) in all aggregateFunctions
            if (!agg.isRewritten() || aggFunctions.isEmpty() || !aggFunctions.stream().allMatch(
                    f -> (f instanceof Min || f instanceof Max)
                            && (f.arity() == 1 && f.child(0).isConstant()))) {
                return null;
            }

            List<NamedExpression> newOutput = agg.getOutputExpressions().stream().map(ne -> {
                if (ne instanceof Alias && ne.child(0) instanceof AggregateFunction) {
                    AggregateFunction f = (AggregateFunction) ne.child(0);
                    if (f instanceof Min || f instanceof Max) {
                        return new Alias(ne.getExprId(), f.child(0), ne.getName());
                    } else {
                        throw new AnalysisException("Unexpected aggregate function: " + f);
                    }
                } else {
                    return ne;
                }
            }).collect(Collectors.toList());

            if (agg.getGroupByExpressions().isEmpty()) {
                LogicalAggregate newAgg = new LogicalAggregate<>(agg.getGroupByExpressions(),
                        agg.getOutputExpressions(), new LogicalLimit(1, 0, LimitPhase.ORIGIN,
                                new LogicalProject<>(newOutput, agg.child())));
                newAgg.setRewritten(true);
                return newAgg;
            } else {
                List<NamedExpression> childOutput = agg.getOutputExpressions().stream().filter(ne ->
                        !(ne instanceof Alias && ne.child(0) instanceof AggregateFunction
                            && (ne.child(0) instanceof Max || ne.child(0) instanceof Min)))
                            .collect(Collectors.toList());
                return new LogicalProject<>(newOutput,
                                new LogicalAggregate<>(agg.getGroupByExpressions(), childOutput, agg.child()));
            }
        }).toRule(RuleType.ELIMINATE_AGGREGATE_CONSTANT);
    }

}
