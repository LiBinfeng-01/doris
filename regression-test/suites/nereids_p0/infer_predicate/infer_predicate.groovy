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

suite("test_infer_predicate") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql 'drop table if exists infer_tb1;'
    sql 'drop table if exists infer_tb2;'

    sql '''create table infer_tb1 (k1 int, k2 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');'''

    sql '''create table infer_tb2 (k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 largeint, k6 date, k7 datetime, k8 float, k9 double) distributed by hash(k1) buckets 3 properties('replication_num' = '1');'''

    res = sql "explain select * from infer_tb1 inner join infer_tb2 where infer_tb2.k1 = infer_tb1.k2  and infer_tb2.k1 = 1;"
    assertFalse(res.contains("k2 = 1"))

    res = sql "explain select * from infer_tb1 inner join infer_tb2 where infer_tb2.k2 = infer_tb1.k1  and infer_tb2.k1 = 1;"
    assertFalse(res.contains("k2 = 1"))

    res = sql "explain select * from infer_tb1 inner join infer_tb2 where infer_tb1.k2 = infer_tb2.k3 and infer_tb2.k3 = 1;"
    assertFalse(res.contains("k2 = 1"))

}
