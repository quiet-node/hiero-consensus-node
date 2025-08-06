package org.hiero.otter.fixtures.app;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.swirlds.logging.legacy.LogMarker.EXCEPTION;
import static com.swirlds.logging.legacy.LogMarker.STARTUP;
import static org.junit.jupiter.api.Assertions.fail;

public class RoundHistory {
    private static final Logger logger = LogManager.getLogger(RoundHistory.class);

    /**
     * A map from round number to historical rounds
     */
    private final Map<Long, ConsistencyServiceRound> roundHistory;

    /**
     * A set of all consensus transactions which have been seen
     */
    private final Set<Long> seenConsensusTransactions;

    /**
     * The location of the log file
     */
    private Path logFilePath;

    /**
     * The writer for the log file
     */
    private BufferedWriter writer;

    /**
     * The round number of the previous round handled
     */
    private long previousRoundHandled;

    /**
     * Constructor
     */
    public RoundHistory() {
        this.roundHistory = new HashMap<>();
        this.seenConsensusTransactions = new HashSet<>();

        // initialization is happening in init()
    }

    /**
     * Initializer
     * <p>
     * Reads the contents of the log file if it exists, and adds the included rounds to the history
     *
     * @param logFilePath the location of the log file
     */
    public void init(@NonNull final Path logFilePath) {
        this.logFilePath = Objects.requireNonNull(logFilePath);

        logger.info(STARTUP.getMarker(), "Consistency testing tool log path: {}", logFilePath);

        tryReadLog();

        try {
            this.writer = new BufferedWriter(new FileWriter(logFilePath.toFile(), true));
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to open writer for transaction handling history", e);
        }
    }

    /**
     * Reads the contents of the log file if it exists, and adds the included rounds to the history
     */
    private void tryReadLog() {
        if (!Files.exists(logFilePath)) {
            logger.info(STARTUP.getMarker(), "No log file found. Starting without any previous history");
            return;
        }

        logger.info(STARTUP.getMarker(), "Log file found. Parsing previous history");

        try (final FileReader in = new FileReader(logFilePath.toFile());
                final BufferedReader reader = new BufferedReader(in)) {
            reader.lines().forEach(line -> {
                final ConsistencyServiceRound parsedRound = ConsistencyServiceRound.fromString(line);

                if (parsedRound == null) {
                    logger.warn(STARTUP.getMarker(), "Failed to parse line from log file: {}", line);
                    return;
                }

                addRoundToHistory(parsedRound);
            });
        } catch (final IOException e) {
            logger.error(EXCEPTION.getMarker(), "Failed to read log file", e);
        }
    }

    /**
     * Add a consensus transaction to the list of seen consensus transactions
     *
     * @param transaction the transaction to add
     * @return an error message if the transaction has already been seen, otherwise null
     */
    @Nullable
    private String addConsensusTransaction(final long transaction) {
        if (!seenConsensusTransactions.add(transaction)) {
            final String error =
                    "Transaction with contents `" + transaction + "` has already been applied to the state";

            logger.error(EXCEPTION.getMarker(), error);
            return error;
        }

        return null;
    }

    /**
     * Compare a newly received round with the historical counterpart. Logs an error if the new round isn't identical to
     * the historical round
     *
     * @param newRound        the round that is being newly processed
     * @param historicalRound the historical round that the new round is being compared to
     * @return an error message if the new round doesn't match the historical round, otherwise null
     */
    @Nullable
    private String compareWithHistoricalRound(
            @NonNull final ConsistencyServiceRound newRound,
            @NonNull final ConsistencyServiceRound historicalRound) {

        Objects.requireNonNull(newRound);
        Objects.requireNonNull(historicalRound);

        if (!newRound.equals(historicalRound)) {
            final String error =
                    "Round " + newRound.roundNumber() + " with transactions " + newRound.transactionsContents()
                            + " doesn't match historical counterpart with transactions "
                            + historicalRound.transactionsContents();

            logger.error(EXCEPTION.getMarker(), error);
            return error;
        }

        return null;
    }

    /**
     * Add a round to the history.
     *
     * @param newRound the round to add
     * @return a list of errors that occurred while adding the round
     */
    @NonNull
    private List<String> addRoundToHistory(@NonNull final ConsistencyServiceRound newRound) {
        final List<String> errors = new ArrayList<>();

        roundHistory.put(newRound.roundNumber(), newRound);

        newRound.transactionsContents().forEach(transaction -> {
            final String error = addConsensusTransaction(transaction);

            if (error != null) {
                errors.add(error);
            }
        });

        if (roundHistory.size() <= 1) {
            previousRoundHandled = newRound.roundNumber();
            // only 1 round is in the history, so no additional checks are necessary
            return errors;
        }

        final long newRoundNumber = newRound.roundNumber();

        // make sure round numbers always increase
        if (newRoundNumber <= previousRoundHandled) {
            final String error = "Round " + newRoundNumber + " is not greater than round " + previousRoundHandled;
            fail("New round number %d is not greater than previous round number %d".formatted(newRoundNumber, previousRoundHandled));

            errors.add(error);
        }

        previousRoundHandled = newRound.roundNumber();

        return errors;
    }

    /**
     * Writes the given round to the log file
     *
     * @param round the round to write to the log file
     */
    private void writeRoundToLog(@NonNull final ConsistencyServiceRound round) {
        Objects.requireNonNull(round);

        try {
            writer.write(round.toString());
            writer.flush();
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to write round `%s` to log".formatted(round.roundNumber()), e);
        }
    }

    /**
     * CHeck the validity of a round.
     * <p>
     * If the input round already exists in the history, this method checks that all transactions are identical to the
     * corresponding historical round
     * <p>
     * If the input round doesn't already exist in the history, this method adds it to the history
     *
     * @param round the round to process
     * @return a list of errors that occurred while processing the round
     */
    @NonNull
    public List<String> checkRoundValidity(@NonNull final ConsistencyServiceRound round) {
        Objects.requireNonNull(round);

        final ConsistencyServiceRound historicalRound = roundHistory.get(round.roundNumber());

        final List<String> errors = new ArrayList<>();
        if (historicalRound == null) {
            // round doesn't already appear in the history, so record it
            errors.addAll(addRoundToHistory(round));
            writeRoundToLog(round);
        } else {
            // if we found a round with the same round number in the round history, make sure the rounds are identical
            final String error = compareWithHistoricalRound(round, historicalRound);

            if (error != null) {
                errors.add(error);
            }
        }

        return errors;
    }

    public void close() {
        try {
            writer.close();
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to close writer for transaction handling history", e);
        }
    }
}
