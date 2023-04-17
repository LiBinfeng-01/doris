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

package org.apache.doris.nereids;

import org.apache.doris.nereids.minidump.Minidump;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class MinidumpTest extends TestWithFeService implements MemoPatternMatchSupported {

    private String path = "/Users/libinfeng/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/minidump/data/";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `t1` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `c` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `c`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `x` int(11) NULL,\n"
                + "  `y` int(11) NULL,\n"
                + "  `z` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`x`, `y`, `z`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`y`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t3` (\n"
                + "  `x` int(11) NULL,\n"
                + "  `y` int(11) NULL,\n"
                + "  `z` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`x`, `y`, `z`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`y`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @Test
    public void testBroadcastJoinHint() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "select * from t1 join [broadcast] t2 on t1.a=t2.x",
                planner -> checkPlannerResult(planner, DistributionMode.BROADCAST)
        );
    }

    @Test
    public void testLoadMinidump() {
        String fileName = MinidumpUtils.generateMinidumpFileName("dumpDemo");
        String filePath = path + fileName;
        Minidump minidump = MinidumpUtils.jsonMinidumpLoad(path, filePath);
        connectContext.setSessionVariable(minidump.getSessionVariable());
        connectContext.setTables(minidump.getTables());
        connectContext.getSessionVariable().setDumpNereids(false);
        PlanChecker.from(connectContext).checkPlannerResult(
                minidump.getSql(),
                planner -> checkMinidumpResult(planner, minidump)
        );
    }

    @Test
    public void testSaveMinidump() {
        connectContext.getSessionVariable().setDumpNereids(true);
        PlanChecker.from(connectContext).checkPlannerResult(
                "select * from t1 join [shuffle] t2 on t1.a=t2.x",
                planner -> checkPlannerResult(planner, DistributionMode.PARTITIONED)
        );
    }

    @Test
    public void testShuffleJoinHint() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "select * from t1 join [shuffle] t2 on t1.a=t2.x",
                planner -> checkPlannerResult(planner, DistributionMode.PARTITIONED)
        );
    }

    private void checkPlannerResult(NereidsPlanner planner, DistributionMode mode) {
        List<PlanFragment> fragments = planner.getFragments();
        Set<HashJoinNode> hashJoins = new HashSet<>();
        for (PlanFragment fragment : fragments) {
            PlanNode plan = fragment.getPlanRoot();
            plan.collect(HashJoinNode.class, hashJoins);
        }

        Assertions.assertEquals(1, hashJoins.size());
        HashJoinNode join = hashJoins.iterator().next();
        Assertions.assertEquals(mode, join.getDistributionMode());
    }

    private void checkMinidumpResult(NereidsPlanner planner, Minidump minidump) {
        Assertions.assertEquals(planner.getParsedPlan().toJson().toString(), minidump.getParsedPlanJson());
        Assertions.assertEquals(planner.getOptimizedPlan().toJson().toString(), minidump.getResultPlanJson());
    }
}
