// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures.consensus.framework.validation;

import static com.swirlds.platform.test.fixtures.consensus.framework.validation.Validations.ValidationType.CONSENSUS_EVENTS;
import static com.swirlds.platform.test.fixtures.consensus.framework.validation.Validations.ValidationType.CONSENSUS_TIMESTAMPS;
import static com.swirlds.platform.test.fixtures.consensus.framework.validation.Validations.ValidationType.DIFFERENT_ORDER;
import static com.swirlds.platform.test.fixtures.consensus.framework.validation.Validations.ValidationType.INPUTS_ARE_SAME;
import static com.swirlds.platform.test.fixtures.consensus.framework.validation.Validations.ValidationType.RATIOS;

import com.swirlds.platform.test.consensus.framework.validation.ConsensusValidator.ConsensusValidationType;
import edu.umd.cs.findbugs.annotations.NonNull;

public class Validations {

    private ConsensusValidator consensusValidator;

    public static @NonNull Validations newInstance() {
        return new Validations();
    }

    public @NonNull Validations consensusAllValidations() {
        consensusValidator = ConsensusValidator.newInstance().standard();
        return this;
    }

    public @NonNull Validations consensusAllValidationsWithRatios(@NonNull final EventRatioValidation ratioValidation) {
        consensusValidator = ConsensusValidator.newInstance().standard().ratios(ratioValidation);
        return this;
    }

    public @NonNull Validations withoutConsensusValidationType(@NonNull final ConsensusValidationType type) {
        consensusValidator.remove(type);
        return this;
    }

    public @NonNull Validations consensusRoundValidations() {
        consensusValidator = ConsensusValidator.newInstance().rounds();
        return this;
    }

    public @NonNull ConsensusValidator getConsensusValidator() {
        return consensusValidator;
    }
}
