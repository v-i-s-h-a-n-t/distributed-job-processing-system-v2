/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.cs324a2.bootstrap;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import com.cs324a2.common.BootstrapInterface;

/**
 *
 * @author Vishant - S11230430
 */
public class BootstrapNode extends UnicastRemoteObject implements BootstrapInterface {

    static final long HEARTBEAT_TIMEOUT_MILLIS = 8_000L;
    private static final long CLEANUP_INTERVAL_MILLIS = 2_000L;
    private final ConcurrentHashMap<Integer, WorkerRegistration> activeWorkers;
    private final ScheduledExecutorService heartbeatMonitor;

    public BootstrapNode() throws RemoteException {
        super();
        this.activeWorkers = new ConcurrentHashMap<>();
        this.heartbeatMonitor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "bootstrap-heartbeat-monitor");
            thread.setDaemon(true);
            return thread;
        });
        heartbeatMonitor.scheduleAtFixedRate(
                this::removeExpiredWorkers,
                CLEANUP_INTERVAL_MILLIS,
                CLEANUP_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized String registerWorker(int workerId, String rmiAddress) throws RemoteException {
        removeExpiredWorkers();
        if (activeWorkers.containsKey(workerId)) {
            throw new RemoteException("Worker ID " + workerId + " is already registered at "
                    + activeWorkers.get(workerId).address() + ". Choose a different worker ID.");
        }

        String randomNeighborAddress = null;

        // If there are already workers in the network, pick a random one for the new node to connect to
        if (!activeWorkers.isEmpty()) {
            List<String> currentNodes = activeWorkers.values().stream()
                    .map(WorkerRegistration::address)
                    .toList();
            int randomIndex = ThreadLocalRandom.current().nextInt(currentNodes.size());
            randomNeighborAddress = currentNodes.get(randomIndex);
        }

        // Add the new worker to the directory
        activeWorkers.put(workerId, new WorkerRegistration(rmiAddress, System.nanoTime()));
        System.out.printf("Bootstrap Worker %d registered successfully at %s%n", workerId, rmiAddress);

        return randomNeighborAddress;
    }

    @Override
    public boolean heartbeat(int workerId) throws RemoteException {
        long now = System.nanoTime();
        return activeWorkers.computeIfPresent(workerId,
                (id, registration) -> registration.withHeartbeat(now)) != null;
    }

    @Override
    public synchronized void deregisterWorker(int workerId) throws RemoteException {
        if (activeWorkers.remove(workerId) != null) {
            System.out.printf("Bootstrap Worker %d deregistered.%n", workerId);
        }
    }

    private void removeExpiredWorkers() {
        long now = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_TIMEOUT_MILLIS);
        activeWorkers.forEach((workerId, registration) -> {
            if (now - registration.lastHeartbeatNanos() > timeoutNanos
                    && activeWorkers.remove(workerId, registration)) {
                System.out.printf("Bootstrap Worker %d at %s expired (missed heartbeats).%n",
                        workerId, registration.address());
            }
        });
    }

    private record WorkerRegistration(String address, long lastHeartbeatNanos) {
        private WorkerRegistration withHeartbeat(long timestamp) {
            return new WorkerRegistration(address, timestamp);
        }
    }

    @Override
    public List<String> getActiveWorkers() throws RemoteException {
        removeExpiredWorkers();
        return activeWorkers.values().stream().map(WorkerRegistration::address).toList();
    }

    private void shutdown(Registry registry) {
        heartbeatMonitor.shutdownNow();
        try {
            registry.unbind("BootstrapNode");
        } catch (RemoteException | NotBoundException e) {
            // best-effort on shutdown
        }
    }

    public static void main(String[] args) {
        try {
            int port = 1099;
            if (args.length > 0) {
                try {
                    port = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port argument '" + args[0] + "', using default " + port);
                }
            }

            Registry registry;

            // Attempt to create the RMI registry programmatically
            try {
                registry = LocateRegistry.createRegistry(port);
                System.out.println("Bootstrap RMI Registry created dynamically on port " + port);
            } catch (RemoteException e) {
                // If it already exists, locate it instead
                registry = LocateRegistry.getRegistry(port);
                System.out.println("Bootstrap Existing RMI Registry located on port " + port);
            }

            // Instantiate and bind the Bootstrap Node to the registry
            BootstrapNode bootstrap = new BootstrapNode();
            registry.rebind("BootstrapNode", bootstrap);
            System.out.println("Bootstrap Node service bound and running. Ready for workers...");

            final Registry finalRegistry = registry;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Bootstrap Node shutting down, unbinding...");
                bootstrap.shutdown(finalRegistry);
            }));
        } catch (Exception e) {
            System.err.println("Bootstrap Critical failure during startup:");
            e.printStackTrace();
        }
    }
}
