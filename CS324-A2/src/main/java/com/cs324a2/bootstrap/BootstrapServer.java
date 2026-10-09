package com.cs324a2.bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Starts the Bootstrap gRPC membership service. */
public final class BootstrapServer {
    public static final int DEFAULT_PORT = 50051;

    private BootstrapServer() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length == 0 ? DEFAULT_PORT : parsePort(args[0]);
        BootstrapGrpcService service = new BootstrapGrpcService();
        Server server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();

        System.out.println("Bootstrap gRPC server listening on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop(server, service),
                "bootstrap-shutdown"));
        server.awaitTermination();
    }

    private static int parsePort(String argument) {
        try {
            int port = Integer.parseInt(argument);
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("port must be between 1 and 65535");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid Bootstrap port: " + argument, e);
        }
    }

    private static void stop(Server server, BootstrapGrpcService service) {
        System.out.println("Stopping Bootstrap gRPC server");
        service.close();
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
