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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.*;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** EliminateAggregateSum
 * with group by:
 * select max(1) from t1 group by c1; -> select 1 from t1;
 * without group by:
 * select max(1) from t1; -> select max(1) from (select 1 from t1 limit 1) tmp;
 *
 * extend:
 * select max(1) + min(c1) from t1 group by c1;
 * */
public class EliminateAggregateSum extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            // check whether we have aggregate(constant) in all aggregateFunctions
            Set<AggregateFunction> aggFunctions = agg.getAggregateFunctions();
            if (!aggFunctions.stream().allMatch(
                    f -> (f instanceof Sum || f instanceof Count || f instanceof Min || f instanceof Max)
                            && (f.arity() == 1 && f.child(0).isConstant()))) {
                return null;
            }

            List<NamedExpression> newOutput = agg.getOutputExpressions().stream().map(ne -> {
                if (ne instanceof Alias && ne.child(0) instanceof AggregateFunction) {
                    AggregateFunction f = (AggregateFunction) ne.child(0);
                    if (f instanceof Sum || f instanceof Min || f instanceof Max) {
                        return new Alias(ne.getExprId(), f.child(0), ne.getName());
                    } else if (f instanceof Count) {
                        return (NamedExpression) ne.withChildren(
                                new If(new IsNull(f.child(0)), Literal.of(0), Literal.of(1)));
                    } else {
                        throw new IllegalStateException("Unexpected aggregate function: " + f);
                    }
                } else {
                    return ne;
                }}).collect(Collectors.toList());
            // make a new project to replace current aggregate
            Plan project =  PlanUtils.projectOrSelf(newOutput, agg.child());
            if (!agg.getGroupByExpressions().isEmpty()) {
                // with group by:
                return project;
            } else {
                // without group by
                return new LogicalSubQueryAlias("tmp", new LogicalLimit<>(1, 0, LimitPhase.ORIGIN, project));
            }
        }).toRule(RuleType.ELIMINATE_AGGREGATE_SUM);
    }

}
