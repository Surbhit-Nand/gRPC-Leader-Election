package com.cs324.computation;

import com.cs324.job.proto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiConsumer;

public final class ClientResultService extends ClientServiceGrpc.ClientServiceImplBase {
    private final BiConsumer<Integer, Object> receive;
    public ClientResultService(BiConsumer<Integer, Object> receive) { this.receive = receive; }
    // Decode the callback for the receiver; the GUI receiver schedules its Swing updates.
    @Override public void deliverResult(TaskResult request, StreamObserver<DeliverResultAck> response) {
        try {
            if (request.getJobId() < 1) throw new IllegalArgumentException("Positive job ID required");
            receive.accept(request.getJobId(), ComputationCodec.result(request));
            response.onNext(DeliverResultAck.getDefaultInstance());
            response.onCompleted();
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
