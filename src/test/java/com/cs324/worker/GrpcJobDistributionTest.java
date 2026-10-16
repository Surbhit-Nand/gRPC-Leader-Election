package com.cs324.worker;

import com.cs324.computation.GrpcJobService;
import com.cs324.computation.GrpcTaskService;
import com.cs324.computation.proto.*;
import io.grpc.*;
import org.junit.jupiter.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** Real TCP gRPC calls exercise serialization, concurrent dispatch and streaming. */
class GrpcJobDistributionTest {
    private final ExecutorService computation = Executors.newFixedThreadPool(4);
    private final List<Server> servers = new ArrayList<>();
    private final List<GrpcJobService.Endpoint> members = new CopyOnWriteArrayList<>();
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private final AtomicInteger assignments = new AtomicInteger();
    private ManagedChannel channel;
    private JobServiceGrpc.JobServiceBlockingStub client;

    @BeforeEach void setup() throws Exception {
        addWorker(2);
        addWorker(3);
        Server coordinator = ServerBuilder.forPort(0).addService(new GrpcJobService(1,
                () -> List.copyOf(members), leader::get, id -> assignments.incrementAndGet(),
                computation, Duration.ofSeconds(5))).build().start();
        servers.add(coordinator);
        channel = ManagedChannelBuilder.forAddress("localhost", coordinator.getPort()).usePlaintext().build();
        client = JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private void addWorker(int id) throws Exception {
        Server server = ServerBuilder.forPort(0).addService(new GrpcTaskService(id, computation)).build().start();
        servers.add(server);
        members.add(new GrpcJobService.Endpoint(id, "localhost", server.getPort()));
    }

    @AfterEach void cleanup() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        for (Server server : servers) server.shutdownNow();
        for (Server server : servers) assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        computation.shutdownNow();
        assertTrue(computation.awaitTermination(5, TimeUnit.SECONDS));
    }

    private JobRequest numbers(JobType type, int... values) {
        Numbers.Builder input = Numbers.newBuilder();
        for (int value : values) input.addValues(value);
        return JobRequest.newBuilder().setJobId(UUID.randomUUID().toString()).setType(type)
                .setInput(WorkInput.newBuilder().setNumbers(input)).build();
    }

    private List<JobUpdate> run(JobRequest request) {
        List<JobUpdate> updates = new ArrayList<>();
        client.submitJob(request).forEachRemaining(updates::add);
        assertTrue(updates.getLast().getFinished());
        assertTrue(updates.getLast().hasResult());
        return updates;
    }

    @Test void maxSplitsAcrossAllWorkersAndStreamsRealCompletion() {
        List<JobUpdate> updates = run(numbers(JobType.MAX, -9, -2, -5, -7, -4, -8));
        assertEquals(-2, updates.getLast().getResult());
        assertEquals(2, assignments.get());
        assertEquals(List.of(1, 2, 3), updates.getFirst().getTasksList().stream()
                .map(TaskProgress::getWorkerId).toList());
        long previous = 0;
        for (JobUpdate update : updates) {
            long completed = update.getTasksList().stream().mapToLong(TaskProgress::getCompletedUnits).sum();
            assertTrue(completed >= previous);
            previous = completed;
            for (TaskProgress task : update.getTasksList())
                assertEquals(task.getCompleted() ? task.getTotalUnits() : 0, task.getCompletedUnits());
            assertEquals(update.getFinished(), update.hasResult());
        }
        assertEquals(6, previous);
    }

    @Test void primeSumUsesInclusiveRangesAndLongResults() {
        JobRequest request = JobRequest.newBuilder().setJobId(UUID.randomUUID().toString()).setType(JobType.PRIMESUM)
                .setInput(WorkInput.newBuilder().setRange(PrimeRange.newBuilder().setStart(2).setEnd(1_000_000))).build();
        assertEquals(37_550_402_023L, run(request).getLast().getResult());
    }

    @Test void primeCountKeepsDuplicatesAndSupportsEmptyInput() {
        assertEquals(4, run(numbers(JobType.PRIMECOUNT, -1, 0, 1, 2, 2, 3, 4, 5)).getLast().getResult());
        List<JobUpdate> empty = run(numbers(JobType.PRIMECOUNT));
        assertEquals(0, empty.getLast().getResult());
        assertEquals(1, empty.getLast().getTasksCount());
    }

    @Test void doesNotDispatchEmptySlicesAndRefreshesMembership() throws Exception {
        assertEquals(1, run(numbers(JobType.MAX, 7)).getFirst().getTasksCount());
        assertEquals(0, assignments.get());
        addWorker(4);
        assertEquals(4, run(numbers(JobType.MAX, 1, 2, 3, 4)).getFirst().getTasksCount());
        assertEquals(3, assignments.get());
    }

    @Test void rejectsInvalidInputAndNonCoordinator() {
        StatusRuntimeException invalid = assertThrows(StatusRuntimeException.class, () -> run(numbers(JobType.MAX)));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalid.getStatus().getCode());
        invalid = assertThrows(StatusRuntimeException.class, () -> run(numbers(JobType.PRIMESUM, 2, 3)));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalid.getStatus().getCode());
        leader.set(false);
        StatusRuntimeException rejected = assertThrows(StatusRuntimeException.class, () -> run(numbers(JobType.MAX, 1)));
        assertEquals(Status.Code.FAILED_PRECONDITION, rejected.getStatus().getCode());
    }

    @Test void concurrentClientsReceiveTheirOwnResults() throws Exception {
        try (ExecutorService clients = Executors.newFixedThreadPool(2)) {
            Future<Long> first = clients.submit(() -> run(numbers(JobType.MAX, 9, 100, -3)).getLast().getResult());
            Future<Long> second = clients.submit(() -> run(numbers(JobType.PRIMECOUNT, 2, 3, 4)).getLast().getResult());
            assertEquals(100L, first.get(10, TimeUnit.SECONDS));
            assertEquals(2L, second.get(10, TimeUnit.SECONDS));
        }
    }

    @Test void workerTransportFailureTerminatesStream() throws Exception {
        servers.getFirst().shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> run(numbers(JobType.MAX, 1, 2, 3)));
        assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
    }

    @Test void coordinatorCanProcessJobsWithoutOtherWorkers() {
        members.clear();
        JobUpdate result = run(numbers(JobType.MAX, 8, 2, 10)).getLast();
        assertEquals(10, result.getResult());
        assertEquals(1, result.getTasksCount());
        assertEquals(1, result.getTasks(0).getWorkerId());
        assertEquals(0, assignments.get());
    }

    @Test void rejectsResultFromTheWrongTask() throws Exception {
        Server wrongWorker = ServerBuilder.forPort(0).addService(new TaskServiceGrpc.TaskServiceImplBase() {
            @Override public void executeTask(TaskRequest request,
                    io.grpc.stub.StreamObserver<TaskResult> response) {
                response.onNext(TaskResult.newBuilder().setJobId(request.getJobId())
                        .setTaskId("another-task").setWorkerId(2).setValue(999).build());
                response.onCompleted();
            }
        }).build().start();
        servers.add(wrongWorker);
        members.clear();
        members.add(new GrpcJobService.Endpoint(2, "localhost", wrongWorker.getPort()));
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> run(numbers(JobType.MAX, 1, 2)));
        assertEquals(Status.Code.DATA_LOSS, error.getStatus().getCode());
    }
}
