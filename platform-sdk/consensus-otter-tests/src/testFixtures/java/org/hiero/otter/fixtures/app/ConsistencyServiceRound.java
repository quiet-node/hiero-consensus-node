package org.hiero.otter.fixtures.app;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.hiero.consensus.model.hashgraph.Round;
import org.hiero.consensus.model.transaction.ConsensusTransaction;

import static org.hiero.base.utility.ByteUtils.byteArrayToLong;
import static org.hiero.otter.fixtures.app.OtterApp.from;
import static org.hiero.otter.fixtures.app.OtterTransaction.DataCase.STATESIGNATURETRANSACTION;

/**
 * A consensus round observed by the ConsistencyService in
 * {@link OtterApp#onHandleConsensusRound(Round, OtterAppState, Consumer)}.
 *
 * @param roundNumber The number of the round
 * @param runningHash The running hash of the consistency service after all the transactions have been added, in the
 * form of a long
 * @param transactionsContents A list of transactions which were included in the round, converted to long values
 */
public record ConsistencyServiceRound(long roundNumber, long runningHash, @NonNull List<Long> transactionsContents)
        implements Comparable<ConsistencyServiceRound> {

    private static final String ROUND_NUMBER_STRING = "Round Number: ";
    private static final String RUNNING_HASH_STRING = "Running Hash: ";
    private static final String TRANSACTIONS_STRING = "Transactions: ";
    private static final String FIELD_SEPARATOR = "; ";
    private static final String LIST_ELEMENT_SEPARATOR = ", ";

    /**
     * Construct a {@link ConsistencyServiceRound} from a {@link Round}
     *
     * @param round the round to convert
     * @param runningHash the running hash value of the application after the round has been applied
     * @return the input round, converted to a {@link ConsistencyServiceRound}
     */
    @NonNull
    public static ConsistencyServiceRound fromRound(@NonNull final Round round, final long runningHash) {
        Objects.requireNonNull(round);

        final List<Long> transactionContents = new ArrayList<>();

        round.forEachTransaction(transaction -> {
            if (isSystemTransaction(transaction)) {
                return;
            }
            transactionContents.add(
                    byteArrayToLong(transaction.getApplicationTransaction().toByteArray(), 0));
        });

        return new ConsistencyServiceRound(round.getRoundNum(), runningHash, transactionContents);
    }

    private static boolean isSystemTransaction(final ConsensusTransaction transaction) {
        final OtterTransaction otterTransaction = Objects.requireNonNull(from(transaction.getApplicationTransaction()));
        return STATESIGNATURETRANSACTION.equals(otterTransaction.getDataCase());
    }

    /**
     * Construct a {@link ConsistencyServiceRound} from a string representation
     *
     * @param roundString the string representation of the round
     * @return the new {@link ConsistencyServiceRound}, or null if parsing failed
     */
    @Nullable
    public static ConsistencyServiceRound fromString(@NonNull final String roundString) {
        Objects.requireNonNull(roundString);

        try {
            final List<String> fields =
                    Arrays.stream(roundString.split(FIELD_SEPARATOR)).toList();

            String field = fields.get(0);
            final long roundNumber = Long.parseLong(field.substring(ROUND_NUMBER_STRING.length()));

            field = fields.get(1);
            final long currentState = Long.parseLong(field.substring(RUNNING_HASH_STRING.length()));

            field = fields.get(2);
            final String transactionsString = field.substring(field.indexOf("[") + 1, field.indexOf("]"));
            final List<Long> transactionsContents = Arrays.stream(transactionsString.split(LIST_ELEMENT_SEPARATOR))
                    .filter(s -> !s.isEmpty())
                    .map(Long::parseLong)
                    .toList();

            return new ConsistencyServiceRound(roundNumber, currentState, transactionsContents);
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NonNull final ConsistencyServiceRound other) {
        Objects.requireNonNull(other);

        return Long.compare(this.roundNumber, other.roundNumber);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(@Nullable final Object other) {
        if (other == null) {
            return false;
        }

        if (other == this) {
            return true;
        }

        if (other instanceof final ConsistencyServiceRound otherRound) {
            return roundNumber == otherRound.roundNumber
                    && runningHash == otherRound.runningHash
                    && transactionsContents.equals(otherRound.transactionsContents);
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(roundNumber, runningHash, transactionsContents);
    }

    /**
     * Produces a string representation of the object that can be parsed by {@link #fromString}.
     * <p>
     * Take care if modifying this method to mirror the change in {@link #fromString}
     *
     * @return a string representation of the object
     */
    @Override
    @NonNull
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append(ROUND_NUMBER_STRING);
        builder.append(roundNumber);
        builder.append(FIELD_SEPARATOR);

        builder.append(RUNNING_HASH_STRING);
        builder.append(runningHash);
        builder.append(FIELD_SEPARATOR);

        builder.append(TRANSACTIONS_STRING);
        builder.append("[");
        for (int index = 0; index < transactionsContents.size(); index++) {
            builder.append(transactionsContents.get(index));
            if (index != transactionsContents.size() - 1) {
                builder.append(LIST_ELEMENT_SEPARATOR);
            }
        }
        builder.append("]\n");

        return builder.toString();
    }
}

