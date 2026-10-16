package com.cs324.bootstrap;

import com.cs324.bootstrap.BootstrapRegistryImpl.RegistrationRecord;
import com.cs324.common.WorkerAddress;
import com.cs324.registry.proto.AllocateJobIdRequest;import com.cs324.registry.proto.AllocateJobIdResponse;
import com.cs324.registry.proto.DeregisterRequest;
import com.cs324.registry.proto.DeregisterResponse;
import com.cs324.registry.proto.GetActiveWorkersRequest;
import com.cs324.registry.proto.GetActiveWorkersResponse;
import com.cs324.registry.proto.GetWorkerAddressRequest;
import com.cs324.registry.proto.GetWorkerAddressResponse;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegisterResponse;
import com.cs324.registry.proto.RegistryServiceGrpc;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.rmi.RemoteException;

// gRPC twin of BootstrapRemote: same directory, same monotonic-id rules,
// second transport. Backed by BootstrapRegistryImpl's record API, so the RMI
// and gRPC paths share one map set and can never diverge.
// Error mapping mirrors the RMI surface: taken ids -> ALREADY_EXISTS
// (was "Duplicate worker id" RemoteException), exhausted job ids ->
// RESOURCE_EXHAUSTED, anything else -> INTERNAL.
public class BootstrapGrpcService extends RegistryServiceGrpc.RegistryServiceImplBase {

    private final BootstrapRegistryImpl directory;

    public BootstrapGrpcService(BootstrapRegistryImpl directory) {
        this.directory = directory;
    }

    @Override
    public void register(RegisterRequest request,
            StreamObserver<RegisterResponse> responseObserver) {
        try {
            RegistrationRecord record = request.getAutoAssign()
                    ? directory.registerAutoRecord(request.getHost(), request.getPort())
                    : directory.registerRecord(
                            request.getWorkerId(), request.getHost(), request.getPort());
            RegisterResponse.Builder reply = RegisterResponse.newBuilder()
                    .setWorkerId(record.workerId())
                    .setHasPeer(record.peerId() != null);
            if (record.peerId() != null) {
                reply.setPeer(WorkerEndpoint.newBuilder()
                        .setWorkerId(record.peerId())
                        .setHost(record.peerAddress().getRmiHost())
                        .setPort(record.peerAddress().getRmiPort()));
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (DuplicateWorkerIdException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage()).asRuntimeException());
        } catch (RemoteException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deregister(DeregisterRequest request,
            StreamObserver<DeregisterResponse> responseObserver) {
        try {
            directory.deregisterWorker(request.getWorkerId());
            responseObserver.onNext(DeregisterResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (RemoteException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void allocateJobId(AllocateJobIdRequest request,
            StreamObserver<AllocateJobIdResponse> responseObserver) {
        try {
            responseObserver.onNext(AllocateJobIdResponse.newBuilder()
                    .setJobId(directory.allocateJobId()).build());
            responseObserver.onCompleted();
        } catch (RemoteException e) {
            Status status = e.getMessage() != null && e.getMessage().contains("exhausted")
                    ? Status.RESOURCE_EXHAUSTED : Status.INTERNAL;
            responseObserver.onError(status.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getActiveWorkers(GetActiveWorkersRequest request,
            StreamObserver<GetActiveWorkersResponse> responseObserver) {
        try {
            responseObserver.onNext(GetActiveWorkersResponse.newBuilder()
                    .addAllWorkerIds(directory.getActiveWorkerIds()).build());
            responseObserver.onCompleted();
        } catch (RemoteException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getWorkerAddress(GetWorkerAddressRequest request,
            StreamObserver<GetWorkerAddressResponse> responseObserver) {
        try {
            WorkerAddress address =
                    directory.getWorkerAddress(request.getWorkerId());
            GetWorkerAddressResponse.Builder reply = GetWorkerAddressResponse.newBuilder()
                    .setFound(address != null);
            if (address != null) {
                reply.setWorker(WorkerEndpoint.newBuilder()
                        .setWorkerId(request.getWorkerId())
                        .setHost(address.getRmiHost())
                        .setPort(address.getRmiPort()));
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (RemoteException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
