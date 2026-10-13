package com.cs324.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.server.UnicastRemoteObject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324.worker.WorkerRemoteImpl;

// Extending UnicastRemoteObject means the constructor exports the worker over
// RMI, so each test unexports it to keep repeated runs from leaking endpoints.
class WorkerRemoteImplTest {

    // The worker each test drives directly, so no registry lookup is needed.
    private WorkerRemoteImpl worker;

    @AfterEach
    void tearDown() {
        // A test that failed early may never have built one.
        if (worker != null) {
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (NoSuchObjectException ignored) {
                // already unexported, nothing to do
            }
        }
    }

    @Test
    // A fresh announcement id is new work, so the recorded coordinator must change.
    void newCoordinatorMessageUpdatesCurrentCoordinator() throws Exception {
        worker = new WorkerRemoteImpl(5);

        worker.receiveCoordinatorMessage(7, 100L);

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "receiving a fresh COORDINATOR message should update who this worker believes is coordinator");
    }

    @Test
    // The id is the dedup key: a repeat carrying a different leader must still be dropped.
    void duplicateCoordinatorMessageIsIgnoredEvenWithADifferentCoordinatorId() throws Exception {
        worker = new WorkerRemoteImpl(5);
        worker.receiveCoordinatorMessage(7, 100L);

        // Same messageId as before but a different coordinator: a duplicate, so drop it.
        worker.receiveCoordinatorMessage(99, 100L);

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "a duplicate messageId must not overwrite the already-recorded coordinator, even with different data");
    }

    @Test
    // Dropping a duplicate must not poison later ids - a new term still has to get through.
    void aGenuinelyNewMessageIdStillUpdatesAfterAnEarlierDuplicateWasIgnored() throws Exception {
        worker = new WorkerRemoteImpl(5);
        worker.receiveCoordinatorMessage(7, 100L);
        worker.receiveCoordinatorMessage(99, 100L); // ignored duplicate

        // A different id entirely: a legitimately new announcement, applied normally.
        worker.receiveCoordinatorMessage(3, 101L);

        assertEquals(3, worker.getCurrentCoordinatorId(),
                "a new messageId should update the coordinator normally, independent of earlier duplicates");
    }

    @Test
    void aFreshWorkerHasItsOwnIdAndNoKnownCoordinator() throws Exception {
        worker = new WorkerRemoteImpl(9);

        assertEquals(9, worker.getId(), "a worker should report the id it was constructed with");
        assertEquals(-1, worker.getCurrentCoordinatorId(),
                "a worker that has seen no COORDINATOR message must not claim to know a coordinator");

        // The election compares JAC, so an unstarted worker must report a zero baseline.
        assertEquals(0, worker.getJobAllocationCounter(),
                "a worker that has coordinated nothing should start with a Job Allocation Counter of zero");
    }

    @Test
    // Discovery polls isActive(), so a running worker must advertise itself.
    void aFreshWorkerReportsItselfActive() throws Exception {
        worker = new WorkerRemoteImpl(4);

        assertTrue(worker.isActive(),
                "a running worker should report itself as active so it can be discovered and handed out as a peer");
    }
}
