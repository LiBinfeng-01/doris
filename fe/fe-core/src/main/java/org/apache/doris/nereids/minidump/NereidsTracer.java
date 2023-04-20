package org.apache.doris.nereids.minidump;// Licensed to the Apache Software Foundation (ASF) under one
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

import com.alibaba.fastjson2.JSON;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.SessionVariable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.sql.Time;
import java.util.List;

/**
 * log consumer
 */
public class NereidsTracer {
    private static long startTime;

    private static final String logFile = "/Users/libinfeng/workspace/doris/fe/log/minidump/dumpDemo/metriclog";

    private static JSONObject totalTraces = new JSONObject();

    private static JSONArray enforcerEvent = new JSONArray();

    private static JSONArray rewriteEvent = new JSONArray();

    private static JSONArray costStateUpdateEvent = new JSONArray();

    private static JSONArray transformEvent = new JSONArray();

    private static JSONArray importantTime = new JSONArray();

    public static void setStartTime(long startTime) {
        NereidsTracer.startTime = startTime;
    }

    public static String getCurrentTime() {
        return String.valueOf(TimeUtils.getEstimatedTime(NereidsTracer.startTime)/1000) + "us";
    }

    public static void logRewriteEvent(String rewriteMsg) {
        JSONObject rewriteEventJson = new JSONObject();
        rewriteEventJson.put(getCurrentTime(), rewriteMsg);
        rewriteEvent.put(rewriteEventJson);
    }

    public static JSONArray getEnforcerEvent() {
        return enforcerEvent;
    }

    public static JSONArray getCostStateUpdateEvent() {
        return costStateUpdateEvent;
    }

    public static JSONArray getTransformEvent() {
        return transformEvent;
    }

    public static void logImportantTime(String eventDesc) {
        JSONObject timeEvent = new JSONObject();
        timeEvent.put(getCurrentTime(), eventDesc);
        importantTime.put(timeEvent);
    }

    public static void output(SessionVariable sessionVariable)
    {
        totalTraces.put("ImportantTime", importantTime);
        totalTraces.put("RewriteEvent", rewriteEvent);
        totalTraces.put("TransformEvent", transformEvent);
        totalTraces.put("CostStateUpdateEvent", costStateUpdateEvent);
        totalTraces.put("EnforcerEvent", enforcerEvent);
        try {
            FileWriter fileWriter = new FileWriter(logFile, true);
            fileWriter.write(totalTraces.toString());
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

