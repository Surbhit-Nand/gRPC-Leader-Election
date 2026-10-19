package com.cs324.client;

import com.cs324.progress.ProgressService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.Test;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class JobProgressReconnectTest {
    @Test void interruptedStreamReconnectsAndUpdatesOnTheEventThread() throws Exception {
        reconnect(false);
    }

    @Test void streamFindsRecoveredJobOnADifferentWorkerPort() throws Exception {
        reconnect(true);
    }

    private void reconnect(boolean moved) throws Exception {
        var service = new ProgressService();
        service.begin(7, Map.of(1, 10L));
        Server server = ServerBuilder.forPort(0).addService(service).build().start();
        int port = server.getPort();
        var model = new DefaultTableModel(new Object[][]{{7, "MAX", "", "Running", "", "Waiting"}},
                new Object[]{"ID", "Type", "Input", "Status", "Result", "Progress"});
        var first = new CountDownLatch(1);
        var done = new CountDownLatch(1);
        var offEdt = new java.util.concurrent.atomic.AtomicBoolean();
        model.addTableModelListener(event -> {
            if (!SwingUtilities.isEventDispatchThread()) offEdt.set(true);
            if (Integer.valueOf(0).equals(model.getValueAt(0, 5))) first.countDown();
            if (Integer.valueOf(100).equals(model.getValueAt(0, 5))) done.countDown();
        });
        var view = new JobProgressView(model);
        var endpoints = new java.util.concurrent.atomic.AtomicReference<java.util.List<com.cs324.registry.proto.WorkerEndpoint>>(java.util.List.of());
        try {
            if (moved) view.watch("localhost", port, 7, 0, () -> true, endpoints::get);
            else view.watch("localhost", port, 7, 0, () -> true);
            assertTrue(first.await(5, TimeUnit.SECONDS));
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            var restored = new ProgressService();
            restored.begin(7, Map.of(2, 10L));
            restored.complete(7, 2, true);
            server = ServerBuilder.forPort(moved ? 0 : port).addService(restored).build().start();
            endpoints.set(java.util.List.of(com.cs324.registry.proto.WorkerEndpoint.newBuilder()
                    .setWorkerId(2).setHost("localhost").setGrpcPort(server.getPort()).build()));
            assertTrue(done.await(8, TimeUnit.SECONDS), "the progress view must resume after an interrupted stream");
            assertFalse(offEdt.get(), "stream callbacks must marshal Swing changes onto the EDT");
        } finally {
            view.close();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
