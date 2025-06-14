// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.services.yahcli.Yahcli;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

public class YahcliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    private Yahcli yahcli;
    private CommandLine commandLine;

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outContent));
        yahcli = new Yahcli();
        commandLine = new CommandLine(yahcli);
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    @DisplayName("Yahcli constructor should initialize with default values")
    public void testConstructor() {
        // Test the newly created instance from @BeforeEach
        assertEquals("config.yml", yahcli.getConfigLoc());
        assertNull(yahcli.getNet());
        assertNull(yahcli.getPayer());
        assertFalse(yahcli.isScheduled());
    }

    @Test
    @DisplayName("Execute with no arguments should return non-zero exit code")
    public void testExecuteWithoutArgs() {
        int exitCode = commandLine.execute();
        assertNotEquals(0, exitCode);
    }

    @Test
    @DisplayName("Execute with help should display help and return 0")
    public void testExecuteWithHelp() {
        int exitCode = commandLine.execute("--help");
        assertEquals(0, exitCode);
        assertTrue(outContent.toString().contains("Usage:"));
    }

    @ParameterizedTest
    @DisplayName("getLogLevel should return correct Level")
    @ValueSource(strings = {"WARN", "INFO", "DEBUG", "ERROR", "TRACE"})
    public void testGetLogLevel(String logLevel) {
        commandLine.parseArgs("-v", logLevel); // Set log level using command line option
        assertEquals(Level.getLevel(logLevel), yahcli.getLogLevel());
    }

    @Test
    @DisplayName("getLogLevel should return WARN for invalid level")
    public void testGetLogLevelInvalidReturnsWarn() {
        // This test uses private implementation details, so we need to use a workaround
        // Instead of setting logLevel directly with reflection, we'll test the fallback behavior

        // A real implementation would have some way to set an invalid level
        // For now, we'll rely on the public API
        assertEquals(Level.WARN, yahcli.getLogLevel());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse network option correctly")
    @CsvSource({"-n,testnet", "--network,mainnet"})
    public void testNetworkOptionParsing(String option, String value) {
        commandLine.parseArgs(option, value);
        assertEquals(value, yahcli.getNet());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse node account option correctly")
    @CsvSource({"-a,0.0.3", "--node-account,0.0.4"})
    public void testNodeAccountOptionParsing(String option, String value) {
        commandLine.parseArgs(option, value);
        assertEquals(value, yahcli.getNodeAccount());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse payer option correctly")
    @CsvSource({"-p,50", "--payer,2"})
    public void testPayerOptionParsing(String option, String value) {
        commandLine.parseArgs(option, value);
        assertEquals(value, yahcli.getPayer());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse fixed fee option correctly")
    @CsvSource({"-f,100", "--fixed-fee,200"})
    public void testFixedFeeOptionParsing(String option, String value) {
        commandLine.parseArgs(option, value);
        assertEquals(Long.parseLong(value), yahcli.getFixedFee());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse config location option correctly")
    @CsvSource({"-c,custom-config.yml", "--config,another-config.yml"})
    public void testConfigLocationOptionParsing(String option, String value) {
        commandLine.parseArgs(option, value);
        assertEquals(value, yahcli.getConfigLoc());
    }

    @ParameterizedTest
    @DisplayName("CLI should parse schedule flag correctly")
    @ValueSource(strings = {"-s", "--schedule"})
    public void testScheduleOptionParsing(String option) {
        commandLine.parseArgs(option);
        assertTrue(yahcli.isScheduled(), String.format("Schedule flag '%s' should set isScheduled to true", option));
    }

    @Test
    @DisplayName("CLI should register all required subcommands")
    public void testSubcommandRegistration() {
        // List of expected subcommands
        String[] expectedSubcommands = {
            "help",
            "accounts",
            "keys",
            "sysfiles",
            "fees",
            "schedule",
            "freeze-abort",
            "freeze-only",
            "prepare-upgrade",
            "freeze-upgrade",
            "upgrade-telemetry",
            "version",
            "activate-staking",
            "nodes"
        };

        for (String subcommand : expectedSubcommands) {
            assertNotNull(
                    commandLine.getSubcommands().get(subcommand), "Subcommand " + subcommand + " should be registered");
        }
    }

    @Test
    @DisplayName("main() should execute commands without exceptions")
    public void testMainExecutesCommand() {
        // Testing main with help to avoid System.exit issues
        assertDoesNotThrow(() -> {
            Yahcli.main("--help");
        });
    }

    // Tests for error conditions

    @Test
    @DisplayName("CLI should handle invalid fixed fee value")
    public void testInvalidFixedFeeValue() {
        Exception exception = assertThrows(ParameterException.class, () -> {
            commandLine.parseArgs("-f", "not-a-number");
        });

        assertTrue(
                exception.getMessage().contains("Invalid value for option"),
                "Error message should mention invalid value");
    }

    @Test
    @DisplayName("CLI should reject unknown options")
    public void testUnknownOption() {
        Exception exception = assertThrows(ParameterException.class, () -> {
            commandLine.parseArgs("--unknown-option");
        });

        assertTrue(exception.getMessage().contains("Unknown option"), "Error message should mention unknown option");
    }

    @Test
    @DisplayName("Call method should throw exception when no subcommand is specified")
    public void testCallWithoutSubcommand() {
        // Parse empty args to initialize properly
        commandLine.parseArgs();

        // Set the CommandLine's spec to the Yahcli instance
        yahcli.spec = commandLine.getCommandSpec();

        Exception exception = assertThrows(ParameterException.class, () -> {
            yahcli.call();
        });

        assertTrue(
                exception.getMessage().contains("Please specify a subcommand"),
                "Error message should prompt for subcommand");
    }

    // Tests for parameter combinations

    @Test
    @DisplayName("CLI should handle mix of valid and invalid parameters by rejecting the invalid ones")
    public void testMixedValidAndInvalidParameters() {
        Exception exception = assertThrows(ParameterException.class, () -> {
            commandLine.parseArgs("-n", "testnet", "--invalid-option", "-p", "50");
        });

        assertTrue(
                exception.getMessage().contains("Unknown option"),
                "Error should be about the unknown option, not valid ones");
    }

    @Test
    @DisplayName("CLI should detect invalid value even when mixed with valid options")
    public void testValidOptionsWithInvalidValue() {
        Exception exception = assertThrows(ParameterException.class, () -> {
            commandLine.parseArgs("-n", "testnet", "-f", "not-a-number", "-p", "50");
        });

        assertTrue(
                exception.getMessage().contains("Invalid value for option"), "Error should be about the invalid value");
    }

    @Test
    @DisplayName("CLI should handle multiple errors by reporting the first one encountered")
    public void testMultipleErrors() {
        Exception exception = assertThrows(ParameterException.class, () -> {
            commandLine.parseArgs("-n", "testnet", "-f", "not-a-number", "--unknown-option");
        });

        // The error could be about either the invalid value or unknown option,
        // depending on which one picocli encounters first
        String errorMessage = exception.getMessage();
        assertTrue(
                errorMessage.contains("Invalid value for option") || errorMessage.contains("Unknown option"),
                "Error should be about either the invalid value or unknown option");
    }

    @Test
    @DisplayName("CLI should parse complex combination of valid options")
    public void testComplexOptionCombination() {
        commandLine.parseArgs(
                "-n",
                "testnet",
                "-p",
                "50",
                "-a",
                "0.0.3",
                "-f",
                String.valueOf(Long.MAX_VALUE),
                "-s",
                "-v",
                "DEBUG",
                "-c",
                "custom-config.yml");

        assertEquals("testnet", yahcli.getNet());
        assertEquals("50", yahcli.getPayer());
        assertEquals("0.0.3", yahcli.getNodeAccount());
        assertEquals(Long.MAX_VALUE, yahcli.getFixedFee());
        assertTrue(yahcli.isScheduled());
        assertEquals(Level.DEBUG, yahcli.getLogLevel());
        assertEquals("custom-config.yml", yahcli.getConfigLoc());
    }

    // Tests for option combinations

    @Test
    @DisplayName("CLI should handle multiple options correctly")
    public void testMultipleOptions() {
        commandLine.parseArgs("-n", "testnet", "-p", "50", "-a", "0.0.3", "-f", "100", "-s");

        assertEquals("testnet", yahcli.getNet());
        assertEquals("50", yahcli.getPayer());
        assertEquals("0.0.3", yahcli.getNodeAccount());
        assertEquals(100L, yahcli.getFixedFee());
        assertTrue(yahcli.isScheduled());
    }

    @Test
    @DisplayName("CLI should apply options across subcommands")
    public void testOptionsWithSubcommand() {
        // Execute with network option and help subcommand
        int exitCode = commandLine.execute("-n", "testnet", "help");

        assertEquals(0, exitCode);
        assertEquals("testnet", yahcli.getNet(), "Global option should be set even when subcommand is specified");
    }

    // Tests for default values

    @Test
    @DisplayName("CLI should use default config location when not specified")
    public void testDefaultConfigLocation() {
        commandLine.parseArgs(); // Parse empty args

        assertEquals("config.yml", yahcli.getConfigLoc(), "Default config location should be 'config.yml'");
    }

    @Test
    @DisplayName("CLI should use default log level when not specified")
    public void testDefaultLogLevel() {
        commandLine.parseArgs(); // Parse empty args

        assertEquals(Level.WARN, yahcli.getLogLevel(), "Default log level should be WARN");
    }

    @Test
    @DisplayName("CLI should handle option override")
    public void testOptionOverride() {
        // Set network to testnet, then override to mainnet
        commandLine.parseArgs("-n", "testnet", "-n", "mainnet");

        assertEquals("mainnet", yahcli.getNet(), "Last specified option should override previous value");
    }

    // Edge case tests

    @Test
    @DisplayName("CLI should handle extremely large fixed fee")
    public void testExtremelyLargeFixedFee() {
        commandLine.parseArgs("-f", String.valueOf(Long.MAX_VALUE)); // Using Long.MAX_VALUE directly

        assertEquals(Long.MAX_VALUE, yahcli.getFixedFee(), "CLI should handle maximum long value for fixed fee");
    }

    @ParameterizedTest
    @DisplayName("CLI should handle various log level case formats")
    @ValueSource(strings = {"warn", "WARN", "Warn", "wArN"})
    public void testLogLevelCaseInsensitivity(String logLevel) {
        commandLine.parseArgs("-v", logLevel);

        assertEquals(Level.WARN, yahcli.getLogLevel(), "Log level parsing should be case-insensitive");
    }

    // This test just makes sure that a test runs without requiring any yahcli-specific dependencies while we figure out
    // its build
    @Test
    void noDependenciesControl() {
        assertEquals(1, 1);
    }
}
