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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Cascades style optimize:
 * Perform equivalent logical plan exploration and physical implementation enumeration,
 * try to find best plan under the guidance of statistic information and cost model.
 */
public class Optimizer {

    private final CascadesContext cascadesContext;

    public Optimizer(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null");
    }

    /**
     * execute optimize, use dphyp or cascades according to join number and session variables.
     */
    public void execute() {
        // if we use leading, logical plan should be replaced before init to memo and join reorder should be forbidden
        Hint leadingHint = cascadesContext.getStatementContext().getHintMap().get("Leading");
        if (leadingHint != null) {
            leadingOptimize((LeadingHint) leadingHint);
        }
        // init memo
        cascadesContext.toMemo();
        // stats derive
        cascadesContext.pushJob(new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        serializeStatUsed(cascadesContext.getConnectContext());
        // DPHyp optimize
        int maxJoinCount = cascadesContext.getMemo().countMaxContinuousJoin();
        cascadesContext.getStatementContext().setMaxContinuousJoin(maxJoinCount);
        boolean isDpHyp = getSessionVariable().enableDPHypOptimizer
                || maxJoinCount > getSessionVariable().getMaxTableCountUseCascadesJoinReorder();
        cascadesContext.getStatementContext().setDpHyp(isDpHyp);
        cascadesContext.getStatementContext().setOtherJoinReorder(false);
        if (!getSessionVariable().isDisableJoinReorder() && isDpHyp) {
            dpHypOptimize();
        }

        // Cascades optimize
        cascadesContext.pushJob(
                new OptimizeGroupJob(cascadesContext.getMemo().getRoot(), cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private Plan generateLeadingJoinPlan(LeadingHint leading) {
        Stack<Pair<Integer, LogicalPlan>> stack = new Stack<>();
        int index = 0;
        LogicalPlan logicalPlan = leading.getTableNameToScanMap().get(leading.getTablelist().get(index));
        logicalPlan = makeFilterPlanIfExist(leading.getFilters(), logicalPlan);
        assert (logicalPlan != null);
        stack.push(Pair.of(leading.getLevellist().get(index), logicalPlan));
        int stackTopLevel = leading.getLevellist().get(index++);
        while (index < leading.getTablelist().size()) {
            int currentLevel = leading.getLevellist().get(index);
            if (currentLevel == stackTopLevel) {
                // should return error if can not found table
                logicalPlan = leading.getTableNameToScanMap().get(leading.getTablelist().get(index++));
                logicalPlan = makeFilterPlanIfExist(leading.getFilters(), logicalPlan);
                Pair<Integer, LogicalPlan> newStackTop = stack.peek();
                while (!(stack.isEmpty() || stackTopLevel != newStackTop.first)) {
                    // check join is legal and get join type
                    JoinType joinType = JoinType.INNER_JOIN;
                    newStackTop = stack.pop();
                    List<Expression> conditions = getJoinConditions(leading.getFilters(), newStackTop.second, logicalPlan);
                    // get joinType
                    LogicalJoin logicalJoin = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION,
                        conditions,
                        JoinHint.NONE,
                        Optional.empty(),
                        newStackTop.second,
                        logicalPlan);
                    if (stackTopLevel > 0) {
                        stackTopLevel--;
                    }
                    if (!stack.isEmpty()) {
                        newStackTop = stack.peek();
                    }
                    logicalPlan = logicalJoin;
                }
                stack.push(Pair.of(stackTopLevel, logicalPlan));
            } else {
                // push
                logicalPlan = leading.getTableNameToScanMap().get(leading.getTablelist().get(index++));
                stack.push(Pair.of(currentLevel, logicalPlan));
                stackTopLevel = currentLevel;
            }
        }

        // we want all filters been remove
        assert (leading.getFilters().isEmpty());
        return stack.pop().second;
    }

    private List<Expression> getJoinConditions(List<Pair<Long,Expression>> filters, LogicalPlan left, LogicalPlan right) {
        List<Expression> joinConditions = new ArrayList<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            Long tablesBitMap = LongBitmap.or(getBitmap(left), getBitmap(right));
            if (LongBitmap.isSubset(filterPair.first, tablesBitMap)) {
                joinConditions.add(filterPair.second);
                filters.remove(i);
            }
        }
        return joinConditions;
    }

    private LogicalPlan makeFilterPlanIfExist(List<Pair<Long,Expression>> filters, LogicalPlan scan) {
        Set<Expression> newConjuncts = new HashSet<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            if (LongBitmap.isSubset(filterPair.first, getBitmap(scan))) {
                newConjuncts.add(filterPair.second);
                filters.remove(i);
            }
        }
        if (newConjuncts.isEmpty()) {
            return scan;
        } else {
            return new LogicalFilter<>(newConjuncts, scan);
        }
    }

    private Long getBitmap(LogicalPlan root) {
        if (root instanceof LogicalJoin) {
            return ((LogicalJoin) root).getBitmap();
        } else if (root instanceof LogicalRelation) {
            return ((LogicalRelation) root).getTable().getId();
        } else if (root instanceof LogicalFilter) {
            return getBitmap((LogicalPlan) root.child(0));
        } else {
            return null;
        }
    }

    // DependsRules: EnsureProjectOnTopJoin.class
    private void leadingOptimize(LeadingHint leading) {
        Plan leadingPlan = generateLeadingJoinPlan(leading);
        cascadesContext.setRewritePlan(leadingPlan);
        getSessionVariable().setDisableJoinReorder(true);
    }

    private void dpHypOptimize() {
        Group root = cascadesContext.getMemo().getRoot();
        // Due to EnsureProjectOnTopJoin, root group can't be Join Group, so DPHyp doesn't change the root group
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        // after DPHyp just keep logical expression
        cascadesContext.getMemo().removePhysicalExpression();
        cascadesContext.getStatementContext().setOtherJoinReorder(true);
    }

    private void serializeStatUsed(ConnectContext connectContext) {
        if (connectContext.getSessionVariable().isPlayNereidsDump()
                || !connectContext.getSessionVariable().isEnableMinidump()) {
            return;
        }
        JSONObject jsonObj = connectContext.getMinidump();
        // add column statistics
        JSONArray columnStatistics = MinidumpUtils.serializeColumnStatistic(
                cascadesContext.getConnectContext().getTotalColumnStatisticMap());
        jsonObj.put("ColumnStatistics", columnStatistics);
        JSONArray histogramArray = MinidumpUtils.serializeHistogram(
                cascadesContext.getConnectContext().getTotalHistogramMap());
        jsonObj.put("Histogram", histogramArray);
    }

    private SessionVariable getSessionVariable() {
        return cascadesContext.getConnectContext().getSessionVariable();
    }
}
