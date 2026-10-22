package com.cs324.worker;

import com.cs324.computation.*;
import com.cs324.common.WorkerEvent;
import com.cs324.job.proto.*;
import com.cs324.registry.proto.WorkerEndpoint;
import com.cs324.progress.ProgressService;
import com.cs324.progress.proto.*;
import io.grpc.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

// Recovery tests use actual TCP gRPC endpoints only.
class GrpcWorkerRecoveryTest {
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private final List<Server> servers = new ArrayList<>();
    private final List<GrpcJobService> services = new ArrayList<>();
    private final List<WorkerEndpoint> members = new CopyOnWriteArrayList<>();
    private final Map<Integer, CompletableFuture<Object>> results = new ConcurrentHashMap<>();
    private final AtomicBoolean unresponsiveWorker = new AtomicBoolean();
    private final AtomicBoolean dropNextReport = new AtomicBoolean();
    private final AtomicInteger reportAttempts = new AtomicInteger();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private final AtomicInteger deliveries = new AtomicInteger();
    private ManagedChannel channel;
    private JobServiceGrpc.JobServiceBlockingStub client;
    private int clientPort;
    private final AtomicInteger jobs = new AtomicInteger();

    @BeforeEach void setup() throws Exception {
        Server callbacks = ServerBuilder.forPort(0).addService(new ClientResultService((id, value) -> {
            deliveries.incrementAndGet();
            results.computeIfAbsent(id, key -> new CompletableFuture<>()).complete(value);
        })).build().start();
        servers.add(callbacks); clientPort=callbacks.getPort();
        Server coordinator=worker(1, pool);
        channel=ManagedChannelBuilder.forAddress("localhost", coordinator.getPort()).usePlaintext().build();
        client=JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }
    private Server worker(int id, Executor executor) throws Exception {
        AtomicInteger port=new AtomicInteger();
        ProgressService progress=new ProgressService();
        GrpcJobService service=new GrpcJobService(id,
                () -> CoordinatorEndpoint.newBuilder().setWorkerId(id).setHost("localhost").setPort(port.get()).build(),
                () -> List.copyOf(members), ignored -> id==1, ignored -> {}, () -> {}, executor,
                progress, Duration.ofSeconds(8), (kind, detail) -> {
                    if (id==1) events.add(kind + ": " + detail);
                });
        Server server=ServerBuilder.forPort(0).addService(ServerInterceptors.intercept(service, new ServerInterceptor() {
            @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                    Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String method=call.getMethodDescriptor().getBareMethodName();
                if (id==2 && unresponsiveWorker.get() && "AssignTask".equals(method)) {
                    // Keep TCP/gRPC alive but never acknowledge this RPC; the deadline must detect it.
                    return new ServerCall.Listener<>() {};
                }
                if (id==1 && "ReportTaskResult".equals(method)) {
                    reportAttempts.incrementAndGet();
                    if (dropNextReport.compareAndSet(true,false)) {
                        call.close(Status.UNAVAILABLE.withDescription("Simulated lost callback"),new Metadata());
                        return new ServerCall.Listener<>() {};
                    }
                }
                return next.startCall(call,headers);
            }
        })).addService(progress).build().start();
        port.set(server.getPort()); servers.add(server); services.add(service);
        members.add(WorkerEndpoint.newBuilder().setWorkerId(id).setHost("localhost").setGrpcPort(port.get()).build());
        return server;
    }
    private SubmitJobRequest submit(String type, Object values) {
        int id=jobs.incrementAndGet();
        results.put(id,new CompletableFuture<>());
        SubmitJobRequest request=SubmitJobRequest.newBuilder().setJobId(id).setJobType(type)
                .setPayload(ComputationCodec.encode(values)).setClient(ClientEndpoint.newBuilder()
                        .setHost("localhost").setPort(clientPort)).build();
        client.submitJob(request);
        return request;
    }
    private Object result(SubmitJobRequest request) throws Exception {
        return results.get(request.getJobId()).get(10,TimeUnit.SECONDS);
    }
    private TaskResult forged(SubmitJobRequest job, int worker, int attempt, int value) {
        TaskResult.Builder result=TaskResult.newBuilder().setJobId(job.getJobId()).setWorkerId(worker)
                .setTaskId(2).setAttempt(attempt);
        if (job.getJobType().equals("PRIMESUM")) result.setLongResult(value); else result.setIntResult(value);
        return result.build();
    }
    @ParameterizedTest
    @CsvSource({"MAX,19", "PRIMESUM,36", "PRIMECOUNT,2"})
    void acceptedWorkerDiesNewWorkerRecoversAndStaleResultsAreFenced(String type, long expected) throws Exception {
        BlockingQueue<Runnable> oldWork=new LinkedBlockingQueue<>();
        Server failed=worker(2, oldWork::add);
        SubmitJobRequest job=submit(type,new int[]{17,19});
        Runnable delayed=oldWork.poll(5,TimeUnit.SECONDS);
        assertNotNull(delayed,"worker accepted active work before dying");
        BlockingQueue<Runnable> newWork=new LinkedBlockingQueue<>();
        worker(3,newWork::add); // Joins after this job was already accepted.
        failed.shutdownNow(); assertTrue(failed.awaitTermination(5,TimeUnit.SECONDS));
        Runnable replacement=newWork.poll(5,TimeUnit.SECONDS);
        assertNotNull(replacement,"fresh discovery must use the newly joined worker");
        client.reportTaskResult(forged(job,2,1,999));
        client.reportTaskResult(forged(job,3,1,999));
        assertFalse(results.get(job.getJobId()).isDone(),"old owners and old attempts cannot finish the job");
        delayed.run(); // Real delayed callback from the old attempt.
        replacement.run();
        assertEquals(expected,((Number)result(job)).longValue());
        client.reportTaskResult(forged(job,3,2,999));
        assertEquals(expected,((Number)result(job)).longValue());
        assertEquals(1,deliveries.get());
        String prefix="Job " + job.getJobId() + ", task 2: ";
        List<String> recoveryEvents=events.stream().filter(e -> e.contains(prefix) && !e.startsWith(WorkerEvent.TASK_DISPATCHED)).toList();
        assertEquals(List.of(
                WorkerEvent.WORKER_FAILED + ": " + prefix + "Worker 2 failed or unreachable; recovering unfinished task [" + type + "]",
                WorkerEvent.TASK_REASSIGNED + ": " + prefix + "reassigned from Worker 2 to Worker 3 (attempt 2) [" + type + "]",
                WorkerEvent.TASK_COMPLETED + ": " + prefix + "reassigned task completed by Worker 3 (attempt 2) [" + type + "]"),
                recoveryEvents, "GUI feed must show ordered recovery events once, despite stale and duplicate results");
        var snapshots=JobProgressServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS)
                .watchJob(WatchJobRequest.newBuilder().setJobId(job.getJobId()).build());
        JobProgress last=null;
        while(snapshots.hasNext()) last=snapshots.next();
        assertNotNull(last); assertTrue(last.getFinished());
        assertEquals(2,last.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum());
        assertTrue(last.getWorkersList().stream().anyMatch(w -> w.getWorkerId()==3));
        assertFalse(last.getWorkersList().stream().anyMatch(w -> w.getWorkerId()==2));
        assertEquals(7,result(submit("MAX",new int[]{7})),"coordinator remains usable");
    }
    @Test void multipleFailedSlicesCanRecoverOnCoordinator() throws Exception {
        BlockingQueue<Runnable> work=new LinkedBlockingQueue<>();
        Server second=worker(2,work::add), third=worker(3,work::add);
        SubmitJobRequest job=submit("PRIMESUM",new int[]{2,3,5,7});
        assertNotNull(work.poll(5,TimeUnit.SECONDS)); assertNotNull(work.poll(5,TimeUnit.SECONDS));
        second.shutdownNow(); third.shutdownNow();
        assertEquals(17L,result(job));
    }
    @Test void healthySlowWorkerIsNotReassignedAndDuplicateProbesDoNotRecalculate() throws Exception {
        BlockingQueue<Runnable> work=new LinkedBlockingQueue<>();
        worker(2,work::add);
        SubmitJobRequest job=submit("PRIMECOUNT",new int[]{2,3});
        Runnable calculation=work.poll(5,TimeUnit.SECONDS); assertNotNull(calculation);
        assertNull(work.poll(1700,TimeUnit.MILLISECONDS),"probes must reuse in-flight work");
        assertFalse(results.get(job.getJobId()).isDone());
        calculation.run(); assertEquals(2,result(job));
    }
    @Test void duplicateResultsDuringActiveJobDoNotDoubleCount() throws Exception {
        BlockingQueue<Runnable> secondWork=new LinkedBlockingQueue<>(), thirdWork=new LinkedBlockingQueue<>();
        worker(2,secondWork::add); worker(3,thirdWork::add);
        SubmitJobRequest job=submit("PRIMESUM",new int[]{2,5,7});
        Runnable second=secondWork.poll(5,TimeUnit.SECONDS), third=thirdWork.poll(5,TimeUnit.SECONDS);
        assertNotNull(second); assertNotNull(third);
        client.reportTaskResult(forged(job,2,1,5));
        client.reportTaskResult(forged(job,2,1,999));
        second.run();
        assertFalse(results.get(job.getJobId()).isDone());
        third.run(); assertEquals(14L,result(job));
    }

    @Test void lostCommunicationRecoversConcurrentJobsWithoutStoppingWorkerServer() throws Exception {
        BlockingQueue<Runnable> work=new LinkedBlockingQueue<>();
        Server failed=worker(2,work::add);
        SubmitJobRequest max=submit("MAX",new int[]{-9,-8,-2,-3});
        SubmitJobRequest sum=submit("PRIMESUM",new WorkerTaskCalculator.PrimeRange(1,1_000_000));
        SubmitJobRequest count=submit("PRIMECOUNT",new int[]{2,2,3,4,5,6});
        for (int i=0;i<3;i++) assertNotNull(work.poll(5,TimeUnit.SECONDS));
        unresponsiveWorker.set(true);
        assertEquals(-2,result(max));
        assertEquals(37_550_402_023L,result(sum));
        assertEquals(4,result(count));
        assertFalse(failed.isShutdown(),"deadline recovery must work even without connection closure");
    }

    @Test void lostResultCallbackIsReplayedWithoutRecalculating() throws Exception {
        BlockingQueue<Runnable> work=new LinkedBlockingQueue<>();
        worker(2,work::add);
        dropNextReport.set(true);
        SubmitJobRequest job=submit("PRIMECOUNT",new int[]{2,3});
        Runnable calculation=work.poll(5,TimeUnit.SECONDS); assertNotNull(calculation);
        calculation.run();
        assertEquals(2,result(job));
        assertTrue(reportAttempts.get()>=2,"cached result must be reported again after the dropped callback");
        assertTrue(work.isEmpty(),"callback retries must not repeat the calculation");
    }

    @AfterEach void cleanup() throws Exception {
        services.forEach(GrpcJobService::close);
        if(channel!=null) channel.shutdownNow().awaitTermination(5,TimeUnit.SECONDS);
        servers.forEach(Server::shutdownNow);
        for(Server server:servers) assertTrue(server.awaitTermination(5,TimeUnit.SECONDS));
        pool.shutdownNow(); assertTrue(pool.awaitTermination(5,TimeUnit.SECONDS));
    }
}
