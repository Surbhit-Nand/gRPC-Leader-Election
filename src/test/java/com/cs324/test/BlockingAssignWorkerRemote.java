package com.cs324.test;

import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// Test double that parks one chosen job inside assignTask until a test releases it.
// The parent acks assignments immediately, so nothing is observable part-way through dispatch.
// Parking one job after the coordinator claims its term slot is the only way to see that window.
// That window is where an unsynchronised term counter would admit a sixth job.
public class BlockingAssignWorkerRemote extends RecordingWorkerRemote {

    private static final long BLOCK_TIMEOUT_SECONDS = 30L;

    // Only the nominated job parks; nothing blocks until a test names a job id.
    private volatile int blockJobId = Integer.MIN_VALUE;
    private volatile CountDownLatch gate = new CountDownLatch(0);

    public BlockingAssignWorkerRemote(int id) throws RemoteException {
        super(id);
    }

    // Arms the gate for one job, and must precede the assignTask expected to park.
    public void blockJob(int jobId) {
        this.blockJobId = jobId;
        this.gate = new CountDownLatch(1);
    }

    // Releasing an unarmed gate is a no-op, so a test can call this from a finally block.
    public void release() {
        gate.countDown();
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        super.assignTask(jobId, jobType, payload);
        // Every other job flows straight through, so the double stays usable for parallel work.
        if (jobId != blockJobId) {
            return;
        }
        try {
            // Bounded, not unbounded: a forgotten release must not hang the whole suite.
            gate.await(BLOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
