// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui;

public record BranchMetadata(Integer branchIndex, Long generation) {

    public Integer getBranchIndex() {
        return branchIndex;
    }

    public Long getGeneration() {
        return generation;
    }
}
