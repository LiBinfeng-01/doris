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

import org.apache.doris.nereids.trees.plans.JoinType;

/**
 * Join constraint which helps for leading to construct outer join , semi join and anti join
 */
public class JoinConstraint {
    private final Long minLeftHand;
    private final Long minRightHand;

    private final Long leftHand;
    private final Long rightHand;

    private final JoinType joinType;

    private final boolean lhsStrict;

    public JoinConstraint(Long minLeftHand, Long minRightHand, Long leftHand, Long rightHand, JoinType joinType, boolean lhsStrict) {
        this.minLeftHand = minLeftHand;
        this.minRightHand = minRightHand;
        this.leftHand = leftHand;
        this.rightHand = rightHand;
        this.joinType = joinType;
        this.lhsStrict = lhsStrict;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Long getLeftHand() {
        return leftHand;
    }

    public Long getRightHand() {
        return rightHand;
    }

    public Long getMinLeftHand() {
        return minLeftHand;
    }

    public Long getMinRightHand() {
        return minRightHand;
    }

    public boolean isLhsStrict() {
        return lhsStrict;
    }
}
