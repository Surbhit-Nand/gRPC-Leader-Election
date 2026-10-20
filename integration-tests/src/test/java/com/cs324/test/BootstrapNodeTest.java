package com.cs324.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Launch bootstrap in a child JVM so its System.exit calls cannot terminate the test runner.
class BootstrapNodeTest {

    // Pass the full test classpath so the child JVM can load gRPC dependencies.
    private static final String CLASSPATH = System.getProperty("java.class.path");
    // A port nothing else in the suite binds, so the child cannot collide with another test.
    private static final int TEST_PORT = 18799;

    // Tracked so tearDown can kill a survivor; a leaked node would keep holding the port.
    private Process runningProcess;
    private final List<Process> processes = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (Process process : processes) {
            if (process.isAlive()) process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "child JVM must terminate");
            }
        }
    }

    private Process startBootstrap(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(System.getProperty("java.home") + "/bin/java");
        cmd.add("-cp");
        cmd.add(CLASSPATH);
        cmd.add("com.cs324.bootstrap.BootstrapNode");
        for (String a : args) cmd.add(a);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        processes.add(process);
        return process;
    }

    private String readAvailableOutput(Process p) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder output = new StringBuilder();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            while (reader.ready()) output.append(reader.readLine()).append("\n");
            if (output.indexOf("RegistryService on port ") >= 0 || !p.isAlive()) break;
            Thread.sleep(25);
        }
        return output.toString();
    }

    @Test
    void startsSuccessfullyOnAFreePort() throws Exception {
        runningProcess = startBootstrap(String.valueOf(TEST_PORT));
        String output = readAvailableOutput(runningProcess);

        assertTrue(runningProcess.isAlive(), "a successfully started Bootstrap Node should keep running, not exit");
        assertTrue(output.contains("RegistryService on port " + TEST_PORT),
                "expected a startup confirmation message, got: " + output);
    }

    @Test
    void exitsCleanlyWithABadPortArgument() throws Exception {
        Process p = startBootstrap("notaport");
        boolean exitedInTime = p.waitFor(5, TimeUnit.SECONDS);

        assertTrue(exitedInTime, "a malformed port argument should fail fast, not hang");
        assertEquals(1, p.exitValue(), "should exit with a non-zero code on failure");
    }

    @Test
    void exitsCleanlyWhenPortIsAlreadyInUse() throws Exception {
        // First process claims the port and stays running.
        runningProcess = startBootstrap(String.valueOf(TEST_PORT));
        readAvailableOutput(runningProcess); // let it finish starting up

        // The second node must print the failure *and* exit; it used to hang forever instead.
        Process second = startBootstrap(String.valueOf(TEST_PORT));
        boolean exitedInTime = second.waitFor(5, TimeUnit.SECONDS);

        assertTrue(exitedInTime, "a port conflict should fail fast and exit, not hang forever");
        assertEquals(1, second.exitValue(), "should exit with a non-zero code on a port conflict");
    }
}
