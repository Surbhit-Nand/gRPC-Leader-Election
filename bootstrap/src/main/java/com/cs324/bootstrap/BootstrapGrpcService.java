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


// The directory's only network transport. Duplicate IDs map to ALREADY_EXISTS; exhausted job IDs map to RESOURCE_EXHAUSTED.
public class BootstrapGrpcService extends RegistryServiceGrpc.RegistryServiceImplBase {

    private final BootstrapRegistryImpl directory;

    public BootstrapGrpcService(BootstrapRegistryImpl directory) {
        this.directory = directory;
    }

    @Override
    public void register(RegisterRequest request,
            StreamObserver<RegisterResponse> responseObserver) {
        try {
            if (request.getGrpcPort() < 0 || request.getGrpcPort() > 65535) {
                throw new IllegalArgumentException("Invalid gRPC port");
            }
            if (request.getUpdateEndpoint()) {
                if (request.getAutoAssign()) throw new IllegalArgumentException("Endpoint update requires a worker ID");
                directory.publishGrpcEndpoint(request.getWorkerId(), request.getHost(), request.getPort(), request.getGrpcPort());
                responseObserver.onNext(RegisterResponse.newBuilder().setWorkerId(request.getWorkerId()).build());
                responseObserver.onCompleted();
                return;
            }
            RegistrationRecord record = directory.registerGrpcRecord(request.getWorkerId(),
                    request.getAutoAssign(), request.getHost(), request.getPort());
            if (request.getGrpcPort() > 0)
                directory.publishGrpcEndpoint(record.workerId(), request.getHost(), request.getPort(), request.getGrpcPort());
            RegisterResponse.Builder reply = RegisterResponse.newBuilder()
                    .setWorkerId(record.workerId())
                    .setHasPeer(record.peerId() != null);
            if (record.peerId() != null) {
                reply.setPeer(WorkerEndpoint.newBuilder()
                        .setWorkerId(record.peerId())
                        .setHost(record.peerAddress().getHost())
                        .setPort(record.peerAddress().getContactPort())
                        .setGrpcPort(record.peerAddress().getGrpcPort()));
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (DuplicateWorkerIdException e) {
            // Duplicate IDs are an explicit protocol error.
            responseObserver.onError(Status.ALREADY_EXISTS
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
        } catch (IllegalStateException e) {
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
        } catch (IllegalStateException e) {
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
        } catch (IllegalStateException e) {
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
                        .setHost(address.getHost())
                        .setPort(address.getContactPort()).setGrpcPort(address.getGrpcPort()));
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (IllegalStateException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
