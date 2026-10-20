package com.cs324.worker;

import com.cs324.bootstrap.*;
import com.cs324.computation.RegistryClient;
import io.grpc.*;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

// Launch the actual worker entry point against a gRPC-only bootstrap.
class GrpcOnlyClusterTest {
    @Test void processesJoinComputeAndRecoverUsingOnlyGrpc() throws Exception {
        var directory = new BootstrapRegistryImpl();
        Server bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(directory)).build().start();
        var processes = new java.util.ArrayList<Process>();
        var logs = new java.util.ArrayList<java.nio.file.Path>();
        var results = new java.util.concurrent.ConcurrentHashMap<Integer, java.util.concurrent.CompletableFuture<Object>>();
        Server callbacks = ServerBuilder.forPort(0).addService(new com.cs324.computation.ClientResultService(
                (id, value) -> results.computeIfAbsent(id, key -> new java.util.concurrent.CompletableFuture<>()).complete(value))).build().start();
        try (RegistryClient client = new RegistryClient("localhost", bootstrap.getPort())) {
            for (int i = 1; i <= 3; i++) {
                var output = Files.createTempFile("grpc-cluster-worker", ".log"); logs.add(output);
                Process process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                        System.getProperty("java.class.path"), WorkerNode.class.getName(), "--grpc-port",
                        String.valueOf(bootstrap.getPort()))
                        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
                processes.add(process);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (process.isAlive() && client.workers().size() < i && System.nanoTime() < deadline) Thread.sleep(50);
                assertTrue(process.isAlive(), Files.readString(output));
                assertEquals(i, client.workers().size());
                // The second and third join a live cluster through the control service.
                awaitLeader(client, java.util.Set.of());
            }
            int leader = awaitLeader(client, java.util.Set.of());
            var monitor = com.cs324.client.ClusterMonitor.poll(client, (id, address) ->
                    new com.cs324.computation.GrpcWorkerClient(client.worker(id)));
            assertEquals(3, monitor.rows().size());
            assertTrue(monitor.rows().stream().allMatch(com.cs324.client.ClusterMonitor.WorkerRow::alive));
            assertTrue(monitor.rows().stream().allMatch(row -> !row.neighbours().isEmpty()));
            var endpoint = client.worker(leader);
            var channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort()).usePlaintext().build();
            int job = client.allocateJobId();
            var result = results.computeIfAbsent(job, key -> new java.util.concurrent.CompletableFuture<>());
            try {
                com.cs324.job.proto.JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                        .submitJob(com.cs324.job.proto.SubmitJobRequest.newBuilder().setJobId(job).setJobType("PRIMESUM")
                                .setPayload(com.cs324.computation.ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 10_000_000)))
                                .setClient(com.cs324.job.proto.ClientEndpoint.newBuilder().setHost("localhost").setPort(callbacks.getPort())).build());
            } finally { channel.shutdownNow(); }
            // Kill an actual JVM after acceptance, so it cannot keep calculating or report a result.
            processes.get(leader - 1).destroyForcibly().waitFor();
            int replacement = awaitLeader(client, java.util.Set.of(leader));
            assertNotEquals(leader, replacement);
            assertEquals(3_203_324_994_356L, result.get(45, TimeUnit.SECONDS));
            var next = client.worker(replacement);
            channel = ManagedChannelBuilder.forAddress(next.getHost(), next.getGrpcPort()).usePlaintext().build();
            try {
                var progress = com.cs324.progress.proto.JobProgressServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(5, TimeUnit.SECONDS).watchJob(com.cs324.progress.proto.WatchJobRequest.newBuilder().setJobId(job).build());
                var last = progress.next();
                while (progress.hasNext()) last = progress.next();
                assertTrue(last.getFinished());
                assertEquals(10_000_000L, last.getWorkersList().stream().mapToLong(com.cs324.progress.proto.WorkerProgress::getCompletedUnits).sum());
            } finally { channel.shutdownNow(); }
        } finally {
            for (Process process : processes) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            callbacks.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            bootstrap.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            for (var log : logs) Files.deleteIfExists(log);
        }
    }

    private int awaitLeader(RegistryClient directory, java.util.Set<Integer> excluded) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            var views = new java.util.HashSet<Integer>();
            for (var endpoint : directory.workers()) {
                if (excluded.contains(endpoint.getWorkerId())) continue;
                try { views.add(new com.cs324.computation.GrpcWorkerClient(endpoint).getCurrentCoordinatorId()); }
                catch (RuntimeException unavailable) { views.add(-1); }
            }
            if (views.size() == 1 && views.iterator().next() > 0 && !excluded.contains(views.iterator().next())) return views.iterator().next();
            Thread.sleep(100);
        }
        throw new AssertionError("gRPC-only workers did not converge");
    }

    @Test void workerStartsAgainstGrpcOnlyBootstrap() throws Exception {
        BootstrapRegistryImpl directory = new BootstrapRegistryImpl();
        Server bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(directory)).build().start();
        var output = Files.createTempFile("grpc-only-worker", ".log");
        Process worker = null;
        try (RegistryClient client = new RegistryClient("localhost", bootstrap.getPort())) {
            worker = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                    System.getProperty("java.class.path"), WorkerNode.class.getName(),
                    "--grpc-port", String.valueOf(bootstrap.getPort()))
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (worker.isAlive() && client.workers().isEmpty() && System.nanoTime() < deadline) Thread.sleep(100);
            assertTrue(worker.isAlive(), Files.readString(output));
            assertEquals(1, client.workers().size(), Files.readString(output));
        } finally {
            if (worker != null) { worker.destroy(); if (!worker.waitFor(5, TimeUnit.SECONDS)) worker.destroyForcibly().waitFor(); }
            bootstrap.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            Files.deleteIfExists(output);
        }
    }
}
