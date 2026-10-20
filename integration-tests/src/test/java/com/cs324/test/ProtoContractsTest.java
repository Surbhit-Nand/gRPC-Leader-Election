package com.cs324.test;

import io.grpc.ServiceDescriptor;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Guards the frozen gRPC contracts: service names, method names and the
// progress service the team already ships. A rename or field-number edit
// breaks this on purpose, forcing an explicit team decision first.
class ProtoContractsTest {

    private static void assertMethods(String serviceName, ServiceDescriptor descriptor,
            String... methods) {
        assertEquals(serviceName, descriptor.getName());
        assertEquals(
                java.util.Set.of(methods),
                descriptor.getMethods().stream()
                        .map(m -> m.getBareMethodName())
                        .collect(Collectors.toSet()),
                "contract methods for " + serviceName + " must not drift");
    }

    @Test
    void registryContractIsFrozen() {
        assertMethods(
                "distrilab.RegistryService",
                com.cs324.registry.proto.RegistryServiceGrpc.getServiceDescriptor(),
                "Register", "Deregister", "AllocateJobId", "GetActiveWorkers", "GetWorkerAddress");
    }

    @Test
    void electionContractIsFrozen() {
        assertMethods(
                "distrilab.ElectionService",
                com.cs324.election.proto.ElectionServiceGrpc.getServiceDescriptor(),
                "SendElection", "SendEcho", "AnnounceCoordinator");
    }

    @Test
    void jobContractIsFrozen() {
        assertMethods(
                "distrilab.JobService",
                com.cs324.job.proto.JobServiceGrpc.getServiceDescriptor(),
                "SubmitJob", "AssignTask", "ReportTaskResult");
        assertMethods(
                "distrilab.ClientService",
                com.cs324.job.proto.ClientServiceGrpc.getServiceDescriptor(),
                "DeliverResult");
    }

    @Test
    void progressContractStillIntact() {
        assertMethods(
                "distrilab.JobProgressService",
                com.cs324.progress.proto.JobProgressServiceGrpc.getServiceDescriptor(),
                "WatchJob");
    }

    @Test
    void payloadShapesCoverBothJobForms() {
        // Range form for PRIMESUM/PRIMECOUNT ranges.
        com.cs324.job.proto.TaskPayload range = com.cs324.job.proto.TaskPayload.newBuilder()
                .setPrimeRange(com.cs324.job.proto.PrimeRangePayload.newBuilder()
                        .setStart(1).setEnd(1_000_000_000))
                .build();
        assertTrue(range.hasPrimeRange());
        assertEquals(1_000_000_000, range.getPrimeRange().getEnd());

        // List form for MAX and listed PRIMECOUNT jobs.
        com.cs324.job.proto.TaskPayload list = com.cs324.job.proto.TaskPayload.newBuilder()
                .setIntList(com.cs324.job.proto.IntList.newBuilder()
                        .addValues(3).addValues(7).addValues(12))
                .build();
        assertTrue(list.hasIntList());
        assertEquals(3, list.getIntList().getValuesCount());

        // Registration round-trips the auto-assign flag and the peer handoff.
        com.cs324.registry.proto.RegisterResponse auto =
                com.cs324.registry.proto.RegisterResponse.newBuilder()
                        .setWorkerId(4).setHasPeer(true)
                        .setPeer(com.cs324.registry.proto.WorkerEndpoint.newBuilder()
                                .setWorkerId(1).setHost("localhost").setPort(3500))
                        .build();
        assertTrue(auto.getHasPeer());
        assertEquals(3500, auto.getPeer().getPort());
    }
}
