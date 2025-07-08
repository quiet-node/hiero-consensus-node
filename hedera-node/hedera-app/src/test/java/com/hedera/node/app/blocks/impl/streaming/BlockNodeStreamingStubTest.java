// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.platform.event.legacy.EventTransaction;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.api.protoc.BlockItemSet;
import org.hiero.block.api.protoc.BlockStreamPublishServiceGrpc;
import org.hiero.block.api.protoc.PublishStreamRequest;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BlockNodeStreamingStubTest {

    private Server server;
    private BlockStreamPublishServiceGrpc.BlockStreamPublishServiceStub stub;

    private int count = 0;

    @BeforeEach
    void setup() throws IOException {
        // Mock implementation of the BlockStreamPublishService
        BlockStreamPublishServiceGrpc.BlockStreamPublishServiceImplBase serviceImpl =
                new BlockStreamPublishServiceGrpc.BlockStreamPublishServiceImplBase() {

                    @Override
                    public StreamObserver<PublishStreamRequest> publishBlockStream(
                            StreamObserver<PublishStreamResponse> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(PublishStreamRequest request) {
                                // No-op for this test, just simulating high-rate data on stream
                                count++;
                            }

                            @Override
                            public void onError(Throwable t) {
                                responseObserver.onError(t);
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        };
                    }
                };

        // Start the gRPC server
        server = ServerBuilder.forPort(8080).addService(serviceImpl).build().start();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.shutdown();
            server.awaitTermination();
        }
    }

    @Test
    @Disabled
    void testHighRateDataPublishing() {
        BlockStreamPublishServiceGrpc.BlockStreamPublishServiceStub stub =
                BlockStreamPublishServiceGrpc.newStub(ManagedChannelBuilder.forAddress("localhost", 8080)
                        .usePlaintext()
                        .build());

        ClientCallStreamObserver<PublishStreamRequest> requestObserver =
                (ClientCallStreamObserver<PublishStreamRequest>) stub.publishBlockStream(new StreamObserver<>() {
                    @Override
                    public void onNext(PublishStreamResponse value) {
                        // No-op for this test, just simulating high-rate data on stream
                    }

                    @Override
                    public void onError(Throwable t) {
                        // No-op for this test, just simulating high-rate data on stream
                    }

                    @Override
                    public void onCompleted() {
                        // No-op for this test, just simulating high-rate data on stream
                    }
                });

        // Create 1MB byte array
        byte[] largeData = new byte[1024 * 1024 * 5]; // 2MB
        ByteString largeDataByteString = ByteString.copyFrom(largeData);

        PublishStreamRequest testRequest = PublishStreamRequest.newBuilder()
                .setBlockItems(BlockItemSet.newBuilder()
                        .addBlockItems(BlockItem.newBuilder()
                                .setEventTransaction(EventTransaction.newBuilder()
                                        .setApplicationTransaction(largeDataByteString)
                                        .build())
                                .build())
                        .build())
                .build();

        // Simulate high-rate data publishing
        for (int i = 0; i < 10000; i++) {
            // Print a Message of Current Status every 1000 messages
            if (i % 1000 == 0) {
                System.out.println("Publishing message number: " + i);
            }
            if (requestObserver.isReady()) {
                requestObserver.onNext(testRequest);
            } else {
                // Park the thread for 10ms in nanos
                LockSupport.parkNanos(10_000_000); // 10 milliseconds in nanoseconds
            }
        }
        requestObserver.onCompleted();
        // Wait for count to reach 10000 with a timeout of 30 seconds
        long startTime = System.currentTimeMillis();
        while (count < 10000 && (System.currentTimeMillis() - startTime) < 30000) {
            // Wait for the count to reach 10000
            LockSupport.parkNanos(100_000_000); // 100 milliseconds in nanoseconds
        }
    }
}
