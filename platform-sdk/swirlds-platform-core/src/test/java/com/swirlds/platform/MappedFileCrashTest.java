// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

public class MappedFileCrashTest {

    private static final int REGION_SIZE = Integer.MAX_VALUE / 100;
    private static final int TOTAL_TEST_FILES = 10;
    private static final String JAVA_FILE_GENERATOR_SOURCE =
            """
            import java.io.*;
            import java.nio.channels.FileChannel;
            import java.nio.MappedByteBuffer;
            import sun.misc.Unsafe;

            public class CrashWriter {
                public static void main(String[] args) throws Exception {
                    System.setProperty("jdk.lang.processExitOnOOM", "true");
                    if (args.length != 1) {
                        System.err.println("Expected one file name argument");
                        System.exit(1);
                    }
                    int REGION_SIZE = %d;
                    java.util.List<Integer> positions = new java.util.ArrayList<>(REGION_SIZE);
                    for (int i = 0; i < REGION_SIZE; i++) {
                        positions.add(i);
                    }
                    java.util.Collections.shuffle(positions, new java.util.Random());

                    File file = new File(args[0]);
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
                         FileChannel fc = raf.getChannel()) {

                        MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, REGION_SIZE);

                        for (int i :positions) {
                            buf.put(i, ((byte)( %s)) );
                        }

                        // No force() call
                        double decision = Math.random();
                        if (decision < 0.5) {
                            System.out.println("Crashing with Runtime.halt()");
                            Runtime.getRuntime().halt(42);
                        } else {
                            System.out.println("Crashing with Unsafe crash (SIGSEGV)");
                            Unsafe.getUnsafe().putAddress(0L, 42L); // SIGSEGV
                        }
                    }
                }
            }
            """;
    public static final Path EXPECTED_FILE = Path.of("expected.dat");
    static Path tempDir;
    static Path javaFile;
    static final String className = "CrashWriter";

    @BeforeAll
    static void before() throws IOException {
        tempDir = Files.createTempDirectory("dynjava");
        javaFile = tempDir.resolve(className + ".java");
        String javaCode = JAVA_FILE_GENERATOR_SOURCE.formatted(REGION_SIZE, "'a' + i%26");

        Files.writeString(javaFile, javaCode);
        System.out.println("Created source at: " + javaFile);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int result = compiler.run(
                null,
                null,
                null,
                "--add-exports",
                "jdk.unsupported/sun.misc=ALL-UNNAMED",
                "-Xlint:-options",
                "-proc:none",
                "-Xlint:none",
                javaFile.toString());
        if (result != 0) {
            throw new RuntimeException("Compilation failed");
        }
    }

    @RepeatedTest(100)
    public void testMappedFilePersistence() throws Exception {
        try {
            createExpectedFile();
            var testFiles = createTestFiles(TOTAL_TEST_FILES);

            compare(EXPECTED_FILE, testFiles);
        } finally {

            Files.deleteIfExists(EXPECTED_FILE);
            IntStream.range(0, TOTAL_TEST_FILES).forEach(i -> {
                try {
                    Files.deleteIfExists(getTestFileName(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static void compare(Path expectedFile, List<Path> files) throws Exception {

        Set<String> checksums = new HashSet<>();
        for (Path file : files) {
            checksums.add(checkSum(file));
        }
        Set<Long> sizes = new HashSet<>();
        for (Path file : files) {
            sizes.add(Files.size(file));
        }
        final String expectedchs = checkSum(expectedFile);
        final long expectedSize = Files.size(expectedFile);
        System.out.printf("Expected checksum:%s obtained: %s%n", expectedchs, checksums);
        System.out.printf("Expected filesize:%s obtained: %s%n", expectedSize, sizes);
        assertEquals(1, checksums.size());
        assertEquals(1, sizes.size());
        assertEquals(expectedchs, checksums.stream().findFirst().get());
        assertEquals(expectedSize, sizes.stream().findFirst().get());
    }

    public static List<Path> createTestFiles(int number) throws Exception {

        final List<Path> results = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            ProcessBuilder pb = new ProcessBuilder(
                    System.getProperty("java.home") + "/bin/java",
                    "--add-exports",
                    "jdk.unsupported/sun.misc=ALL-UNNAMED",
                    "-cp",
                    tempDir.toString(),
                    className,
                    "crash-test" + i + ".dat");
            pb.inheritIO();
            Process process = pb.start();
            int exit = process.waitFor();
            System.out.println("Process exited with code: " + exit);
            results.add(getTestFileName(i));
        }
        return results;
    }

    private static Path getTestFileName(final int i) {
        return Path.of("crash-test" + i + ".dat");
    }

    /**
     * Compute SHA256 checksum of a file (Linux/macOS)
     * @param val
     * @return
     * @throws Exception
     */
    static String checkSum(Path val) throws Exception {
        ProcessBuilder pb =
                new ProcessBuilder("shasum", "-a", "256", val.toFile().getAbsolutePath());
        Process process = pb.start();

        String line;
        StringBuilder builder = new StringBuilder();
        // Read output
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            while ((line = reader.readLine()) != null) {
                builder.append(line.split(" ")[0]);
            }
        }
        return builder.toString();
    }

    private static void createExpectedFile() {
        try (RandomAccessFile raf = new RandomAccessFile(MappedFileCrashTest.EXPECTED_FILE.toFile(), "rw");
                FileChannel fc = raf.getChannel()) {

            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, REGION_SIZE);
            for (int i = 0; i < REGION_SIZE; i++) {
                buf.put(i, (byte) ('a' + i % 26));
            }
            buf.force();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
