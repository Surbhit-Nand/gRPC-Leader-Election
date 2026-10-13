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

// BootstrapNode.main() calls System.exit() on failure, which would kill the test JVM itself.
// Each test therefore launches it as a real child process, the way it is actually run.
// Its exit code and console output are then read from the outside, not in-process.
class BootstrapNodeTest {

    // Maven puts compiled classes here by default.
    private static final String CLASSPATH = "target/classes";
    // A port nothing else in the suite binds, so the child cannot collide with another test.
    private static final int TEST_PORT = 18799;

    // Tracked so tearDown can kill a survivor; a leaked node would keep holding the port.
    private Process runningProcess;

    @AfterEach
    void tearDown() {
        if (runningProcess != null && runningProcess.isAlive()) {
            runningProcess.destroy();
        }
    }

    private Process startBootstrap(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("java");
        cmd.add("-cp");
        cmd.add(CLASSPATH);
        cmd.add("com.cs324.bootstrap.BootstrapNode");
        for (String a : args) cmd.add(a);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        return pb.start();
    }

    private String readAvailableOutput(Process p) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder output = new StringBuilder();
        // Give it a moment to actually print before we read.
        Thread.sleep(1500);
        while (reader.ready()) {
            output.append(reader.readLine()).append("\n");
        }
        return output.toString();
    }

    @Test
    void startsSuccessfullyOnAFreePort() throws Exception {
        runningProcess = startBootstrap(String.valueOf(TEST_PORT));
        String output = readAvailableOutput(runningProcess);

        assertTrue(runningProcess.isAlive(), "a successfully started Bootstrap Node should keep running, not exit");
        assertTrue(output.contains("running on port " + TEST_PORT),
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