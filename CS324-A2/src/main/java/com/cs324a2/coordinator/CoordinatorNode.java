package com.cs324a2.coordinator;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.compute.ResultAggregator;
import com.cs324a2.compute.WorkPartitioner;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateResponse;
import com.cs324a2.grpc.coordinator.JobProgressRequest;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobSpec;
import com.cs324a2.grpc.coordinator.JobStateProto;
import com.cs324a2.grpc.coordinator.JobStatus;
import com.cs324a2.grpc.coordinator.WorkerProgress;
import com.cs324a2.scheduler.JobExecutionException;
import com.cs324a2.scheduler.SchedulerConfig;
import com.cs324a2.scheduler.WorkerMembership;
import com.cs324a2.scheduler.WorkerRpcClient;
import com.cs324a2.worker.WorkerGrpcService;
import com.cs324a2.worker.WorkerProtoMapper;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Combined worker + coordinator node.
 *
 * <p>Each node serves task execution ({@code WorkerService}) and coordinator
 * duties ({@code CoordinatorService}) on one gRPC port, so every election RPC
 * (terms, votes, heartbeats, replication, submit) travels over gRPC.
 *
 * <p>Election is the Assignment-1 unstructured flood, re-expressed over gRPC:
 * <ul>
 *   <li>Workers form an unstructured overlay: each node links to a subset of
 *       peers ({@code neighbours}); a joining node connects to one random
 *       active worker (bilateral {@code AddNeighbor}).</li>
 *   <li>When no coordinator is active any worker may initiate an election.
 *       An {@code ELECTION} message floods the overlay (neighbours except
 *       sender); duplicates are ignored via {@code seenElectionIds}.</li>
 *   <li>Convergecast returns the best candidate of each reached subtree; all
 *       reachable active workers are considered.</li>
 *   <li>Lowest Job Allocation Counter (JAC) wins; ties break to the highest
 *       worker ID.</li>
 *   <li>The winner is announced with a {@code COORDINATOR} flood so every
 *       reachable worker eventually agrees on one leader per term.</li>
 *   <li>A leader serves one term of at most {@code MAX_JOBS_PER_TERM} jobs;
 *       in-flight jobs finish first, then a new election is triggered.</li>
 * </ul>
 *
 * <p>Monotonic leader {@code terms} are kept for agreement (stale terms are
 * rejected, only the first leader per term is recognised). The legacy
 * Raft-style {@code RequestVote} quorum handlers are retained for
 * compatibility but the JAC flood is the primary election path.
 *
 * <p>Job checkpoints (job spec, per-task states, results, progress event
 * numbers) are replicated to followers on every mutation. A newly elected
 * leader resumes unfinished jobs from its local replica without client help.
 *
 * <p>The injected {@link WorkerMembership} and {@link WorkerRpcClient} remain
 * owned by the caller and are not closed with this node.
 */
public final class CoordinatorNode implements AutoCloseable {
    static final long HEARTBEAT_INTERVAL_MILLIS = 150;
    static final long ELECTION_TIMEOUT_BASE_MILLIS = 500;
    static final long ELECTION_TIMEOUT_JITTER_MILLIS = 500;
    private static final long RPC_TIMEOUT_MILLIS = 800;
    private static final long REPLICATE_TIMEOUT_MILLIS = 1_000;
    /** One coordinator term serves at most this many jobs (Assignment 1). */
    public static final int MAX_JOBS_PER_TERM = 5;
    /** Sub-ranges per worker for PRIMESUM, so progress advances smoothly. */
    static final int RANGE_PARTITIONS_PER_WORKER = 4;
    /** Upper bound on task count, keeping snapshot replication cost bounded. */
    static final int MAX_PARTITIONS_PER_JOB = 32;

    private final int workerId;
    private final String host;
    private final int port;
    private final WorkerMembership membership;
    private final WorkerRpcClient taskClient;
    private final SchedulerConfig config;

    private final ReplicatedJobStore store = new ReplicatedJobStore();
    private final CoordinatorRpcClient coordClient = new CoordinatorRpcClient();
    private final WorkerGrpcService taskService;
    private final CoordinatorServiceImpl coordService;
    private Server server;

    private final Object stateLock = new Object();
    private long currentTerm;
    private Integer votedFor;
    private Integer leaderId;
    /** JAC of the recognised leader; used for deterministic same-term merges. */
    private int leaderJac = Integer.MAX_VALUE;
    private long leaderProgressIndex;
    private boolean isLeader;
    private volatile boolean leaderReady = true;
    private volatile long lastHeartbeatNanos = System.nanoTime();
    private final long electionTimeoutMillis =
            ELECTION_TIMEOUT_BASE_MILLIS
                    + ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_JITTER_MILLIS + 1);
    private volatile long lastHeartbeatSentNanos;
    private final AtomicBoolean electionInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    // ---- Assignment-1 JAC election state (unstructured flood over gRPC) ----
    /** Job Allocation Counter: incremented per job assigned as coordinator. */
    private final AtomicInteger jac = new AtomicInteger(0);
    private final AtomicInteger jobsInCurrentTerm = new AtomicInteger(0);
    private final Object termStateLock = new Object();
    private boolean termClosing = false;
    private boolean termTransitionStarted = false;
    private int inFlightJobs = 0;
    private final AtomicLong electionSeq = new AtomicLong(0);
    private final Set<String> seenElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> seenCoordinatorIds = ConcurrentHashMap.newKeySet();
    private final Object electionLock = new Object();
    /** Unstructured overlay: subset of peers this node forwards to. */
    private final CopyOnWriteArrayList<WorkerEndpoint> neighbors =
            new CopyOnWriteArrayList<>();
    /** Consecutive RPC failures per neighbour; drop at threshold (§7.1). */
    private final ConcurrentHashMap<Integer, AtomicInteger> neighborFailures =
            new ConcurrentHashMap<>();
    /** Neighbours are dropped after this many consecutive failures. */
    static final int MAX_NEIGHBOR_FAILURES = 3;
    private volatile Thread membershipWatcher;
    private final AtomicLong membershipWatcherVersion = new AtomicLong(-1);

    private final ScheduledExecutorService timers;
    private final ExecutorService jobs;
    private final AtomicInteger workerCursor = new AtomicInteger();
    private final ConcurrentHashMap<String, AtomicInteger> dispatchFailures =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> runningJobs = new ConcurrentHashMap<>();
    private final AtomicLong electionCount = new AtomicLong();

    public CoordinatorNode(int workerId, String host, int port,
                           WorkerMembership membership, WorkerRpcClient taskClient,
                           SchedulerConfig config) {
        if (workerId <= 0) throw new IllegalArgumentException("worker ID must be positive");
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("port must be 1-65535");
        if (membership == null) throw new IllegalArgumentException("membership must not be null");
        if (taskClient == null) throw new IllegalArgumentException("task client must not be null");
        if (config == null) throw new IllegalArgumentException("config must not be null");
        this.workerId = workerId;
        this.host = host.trim();
        this.port = port;
        this.membership = membership;
        this.taskClient = taskClient;
        this.config = config;
        this.taskService = new WorkerGrpcService(workerId);
        this.coordService = new CoordinatorServiceImpl(this);
        this.timers = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "coordinator-" + workerId + "-timers");
            thread.setDaemon(true);
            return thread;
        });
        this.jobs = Executors.newFixedThreadPool(Math.max(4, config.dispatchThreads()), task -> {
            Thread thread = new Thread(task, "coordinator-" + workerId + "-jobs");
            thread.setDaemon(false);
            return thread;
        });
    }

    public int workerId() {
        return workerId;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(taskService)
                .addService(coordService)
                .build()
                .start();
        taskService.startAccepting();
        lastHeartbeatNanos = System.nanoTime()
                + ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_JITTER_MILLIS);
        timers.scheduleWithFixedDelay(this::tick, 25, 25, TimeUnit.MILLISECONDS);
        startMembershipWatcher();
        System.out.printf("[Coordinator %d] started at %s:%d JAC=%d term=%d neighbours=%s%n",
                workerId, host, port, jac.get(), getCurrentTerm(), neighborIdsString());
    }

    /**
     * Watches Bootstrap membership and prunes neighbour links whose workers
     * left or expired (§7.1), so floods stop dialling dead peers. Runs as a
     * daemon; failures here never disturb the timer or job pools.
     */
    private void startMembershipWatcher() {
        Thread watcher = new Thread(() -> {
            long version = membershipWatcherVersion.get();
            try {
                version = membership.snapshot().version();
                membershipWatcherVersion.set(version);
            } catch (RuntimeException e) {
                version = -1;
            }
            while (!closed.get()) {
                try {
                    long next = membership.awaitChange(version,
                            Duration.ofSeconds(5));
                    version = next;
                    membershipWatcherVersion.set(next);
                    pruneNeighborsToActive();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "coordinator-" + workerId + "-membership-watch");
        watcher.setDaemon(true);
        membershipWatcher = watcher;
        watcher.start();
    }

    /** Drops neighbour links absent from the active Bootstrap snapshot. */
    void pruneNeighborsToActive() {
        java.util.Set<Integer> active;
        try {
            active = new java.util.HashSet<>();
            for (WorkerEndpoint w : membership.snapshot().workers()) {
                active.add(w.getWorkerId());
            }
        } catch (RuntimeException e) {
            return;
        }
        for (WorkerEndpoint n : neighbors) {
            if (!active.contains(n.getWorkerId())) {
                removeNeighborEndpoint(n.getWorkerId());
                System.out.printf("[Coordinator %d] neighbours: pruned departed worker %d"
                                + " — neighbours now %s%n",
                        workerId, n.getWorkerId(), neighborIdsString());
            }
        }
    }

    /** Removes one neighbour link. Returns true if one was present. */
    public boolean removeNeighborEndpoint(int neighbourId) {
        boolean removed = neighbors.removeIf(n -> n.getWorkerId() == neighbourId);
        neighborFailures.remove(neighbourId);
        return removed;
    }

    private void recordNeighborSuccess(int neighbourId) {
        neighborFailures.remove(neighbourId);
    }

    /**
     * Counts a failed RPC to a neighbour and drops the link after
     * {@link #MAX_NEIGHBOR_FAILURES} consecutive failures (§7.1).
     */
    private void recordNeighborFailure(int neighbourId) {
        AtomicInteger count = neighborFailures.computeIfAbsent(neighbourId,
                k -> new AtomicInteger(0));
        if (count.incrementAndGet() >= MAX_NEIGHBOR_FAILURES) {
            if (removeNeighborEndpoint(neighbourId)) {
                System.out.printf("[Coordinator %d] neighbours: dropped unreachable worker %d"
                                + " after %d consecutive failures — neighbours now %s%n",
                        workerId, neighbourId, MAX_NEIGHBOR_FAILURES, neighborIdsString());
            }
        }
    }

    // ---- Client entry points ----

    /**
     * Submits a job. Leaders execute; followers forward to the known leader
     * over gRPC. When no live coordinator exists, a JAC flood election is
     * initiated first (any worker may initiate), then forwarding is retried
     * once before failing with the leader hint.
     */
    public long submitJob(JobRequest request) throws JobExecutionException {
        if (request == null) throw new IllegalArgumentException("request must not be null");
        if (closed.get()) throw new JobExecutionException("coordinator node is closed");
        if (isLeader()) {
            return executeNewJob(request);
        }
        if (!hasLiveLeader()) {
            System.out.printf("[Coordinator %d] no live coordinator — worker %d initiating"
                            + " on-demand election for job %s%n",
                    workerId, workerId, request.jobId());
            try {
                initiateElection();
            } catch (RuntimeException e) {
                System.err.printf("[Coordinator %d] on-demand election failed: %s%n",
                        workerId, e.getMessage());
            }
            if (isLeader()) {
                return executeNewJob(request);
            }
        }
        awaitLocalLeaderReady();
        if (isLeader()) return executeNewJob(request);
        LeaderHint hint = leaderHint();
        if (hint != null && hint.workerId() != workerId) {
            try {
                CoordinatorJobResponse response = coordClient.submit(
                        hint.host(), hint.port(),
                        CoordinatorJobRequest.newBuilder()
                                .setJob(CoordinatorJobMapper.toSpec(request)).build(),
                        config.taskTimeout().toMillis() + config.jobTimeout().toMillis());
                if (response.getExecuted()) {
                    if (!response.getError().isEmpty()) {
                        throw new JobExecutionException("job " + request.jobId()
                                + " failed on leader " + hint.workerId() + ": "
                                + response.getError());
                    }
                    return response.getResultValue();
                }
            } catch (StatusRuntimeException e) {
                // Leader hint is stale: try one election, then one retry.
                try {
                    initiateElection();
                } catch (RuntimeException ignored) {
                    // Fall through to the hinted failure below.
                }
                LeaderHint fresh = leaderHint();
                if (fresh != null && fresh.workerId() != workerId
                        && fresh.host() != null && fresh.port() > 0) {
                    try {
                        CoordinatorJobResponse retry = coordClient.submit(
                                fresh.host(), fresh.port(),
                                CoordinatorJobRequest.newBuilder()
                                        .setJob(CoordinatorJobMapper.toSpec(request)).build(),
                                config.taskTimeout().toMillis() + config.jobTimeout().toMillis());
                        if (retry.getExecuted()) {
                            if (!retry.getError().isEmpty()) {
                                throw new JobExecutionException("job " + request.jobId()
                                        + " failed on leader " + fresh.workerId() + ": "
                                        + retry.getError());
                            }
                            return retry.getResultValue();
                        }
                    } catch (StatusRuntimeException ignored) {
                        // Fall through to the hinted failure below.
                    }
                }
            }
        }
        LeaderHint hintNow = leaderHint();
        throw new NotLeaderException("worker " + workerId + " is not the leader",
                leaderId(), hintNow == null ? null : hintNow.host(),
                hintNow == null ? -1 : hintNow.port());
    }

    // ---- gRPC handlers (delegated from CoordinatorServiceImpl) ----

    record VoteOutcome(long term, boolean granted) {}
    record HeartbeatOutcome(long term, boolean accepted) {}
    record ReplicateOutcome(long term, boolean accepted) {}

    VoteOutcome handleRequestVote(long term, int candidateId, long candidateProgress) {
        synchronized (stateLock) {
            if (term < currentTerm) return new VoteOutcome(currentTerm, false);
            if (term > currentTerm) stepDownLocked(term);
            // Single vote per term.
            if (votedFor != null && !votedFor.equals(candidateId)) {
                return new VoteOutcome(currentTerm, false);
            }
            // Only an up-to-date candidate may win, so the successor can
            // restore replicated jobs, tasks, results, and progress numbers.
            if (candidateProgress < store.progress()) {
                return new VoteOutcome(currentTerm, false);
            }
            // Do not disrupt a live leader holding this term.
            long sinceHeartbeat = System.nanoTime() - lastHeartbeatNanos;
            if (leaderId != null && !leaderId.equals(candidateId)
                    && sinceHeartbeat < TimeUnit.MILLISECONDS.toNanos(electionTimeoutMillis)) {
                return new VoteOutcome(currentTerm, false);
            }
            votedFor = candidateId;
            lastHeartbeatNanos = System.nanoTime();
            System.out.printf("[Coordinator %d] voted for %d in term %d (progress=%d)%n",
                    workerId, candidateId, currentTerm, candidateProgress);
            return new VoteOutcome(currentTerm, true);
        }
    }

    /** Backwards-compatible heartbeat (unknown leader JAC). */
    HeartbeatOutcome handleHeartbeat(long term, int heartbeatLeaderId,
                                     String leaderHost, int leaderPort,
                                     long commitProgress) {
        return handleHeartbeat(term, heartbeatLeaderId, leaderHost, leaderPort,
                commitProgress, Integer.MAX_VALUE);
    }

    HeartbeatOutcome handleHeartbeat(long term, int heartbeatLeaderId,
                                     String leaderHost, int leaderPort,
                                     long commitProgress, int heartbeatLeaderJac) {
        synchronized (stateLock) {
            if (term < currentTerm) return new HeartbeatOutcome(currentTerm, false);
            if (term > currentTerm) {
                stepDownLocked(term);
                leaderId = heartbeatLeaderId;
                leaderJac = heartbeatLeaderJac;
                lastHeartbeatNanos = System.nanoTime();
                store.observeProgress(commitProgress);
                return new HeartbeatOutcome(currentTerm, true);
            }
            if (leaderId == null) {
                leaderId = heartbeatLeaderId;
                leaderJac = heartbeatLeaderJac;
                lastHeartbeatNanos = System.nanoTime();
                store.observeProgress(commitProgress);
                return new HeartbeatOutcome(currentTerm, true);
            }
            if (!leaderId.equals(heartbeatLeaderId)) {
                // Split-brain protection: only the first leader per term is valid.
                System.err.printf("[Coordinator %d] rejected second leader %d for term %d"
                                + " (recognised leader=%d)%n",
                        workerId, heartbeatLeaderId, currentTerm, leaderId);
                return new HeartbeatOutcome(currentTerm, false);
            }
            leaderJac = heartbeatLeaderJac;
            lastHeartbeatNanos = System.nanoTime();
            // Heartbeat progress is a hint, not a replicated checkpoint.
            // Advancing the local watermark here could let an empty replica
            // appear up to date in a subsequent election.
            return new HeartbeatOutcome(currentTerm, true);
        }
    }

    ReplicateOutcome handleReplicate(long term, int replicateLeaderId,
                                     long progressIndex, JobStateProto jobState) {
        boolean valid;
        synchronized (stateLock) {
            if (term < currentTerm) return new ReplicateOutcome(currentTerm, false);
            if (term > currentTerm) {
                stepDownLocked(term);
                leaderId = replicateLeaderId;
            } else if (leaderId == null) {
                leaderId = replicateLeaderId;
            } else if (!leaderId.equals(replicateLeaderId)) {
                return new ReplicateOutcome(currentTerm, false);
            }
            lastHeartbeatNanos = System.nanoTime();
            valid = true;
        }
        if (valid) {
            store.applyReplicated(jobState, progressIndex);
            return new ReplicateOutcome(currentTerm, true);
        }
        return new ReplicateOutcome(currentTerm, false);
    }

    CoordinatorJobResponse handleSubmit(JobSpec spec) {
        if (!isLeader() && !hasLiveLeader()) {
            initiateElection();
        }
        awaitLocalLeaderReady();
        CoordinatorJobResponse.Builder response = CoordinatorJobResponse.newBuilder()
                .setTerm(getCurrentTerm());
        LeaderHint hint = leaderHint();
        response.setLeaderId(hint == null ? -1 : hint.workerId());
        if (hint != null) {
            response.setLeaderHost(hint.host()).setLeaderPort(hint.port());
        }
        if (!isLeader()) {
            return response.setExecuted(false).build();
        }
        JobRequest request;
        try {
            request = CoordinatorJobMapper.toJobRequest(spec);
        } catch (IllegalArgumentException e) {
            return response.setExecuted(true).setError(e.getMessage()).build();
        }
        try {
            long result = executeNewJob(request);
            ReplicatedJobStore.JobRecord finished = store.get(request.jobId());
            var completedResponse = response.setExecuted(true).setResultValue(result).setHasResult(true);
            if (finished != null && !finished.exactResultValue().isEmpty()) {
                completedResponse.setExactResultValue(finished.exactResultValue());
            }
            return completedResponse
                    .setProgressIndex(finished == null ? store.progress()
                            : finished.progressIndex())
                    .build();
        } catch (NotLeaderException e) {
            return response.setExecuted(false).build();
        } catch (JobExecutionException e) {
            ReplicatedJobStore.JobRecord failed = store.get(request.jobId());
            // A coordinator may shut down after accepting the RPC. Its local
            // dispatch abort is not a job failure: the replicated checkpoint
            // remains active and the replacement leader can finish it.
            if ((closed.get() || !isLeader())
                    && failed != null
                    && failed.status() != ReplicatedJobStore.JobStatus.FAILED) {
                return response.setExecuted(false).build();
            }
            var builder = response.setExecuted(true)
                    .setError(e.getMessage() == null ? "job failed" : e.getMessage());
            if (failed != null) builder.setProgressIndex(failed.progressIndex());
            return builder.build();
        }
    }

    GetCoordinatorStateResponse handleGetState() {
        int jacNow = jac.get();
        int termCountNow = jobsInCurrentTerm.get();
        int leaderJacNow;
        List<Integer> neighbourIds = new ArrayList<>();
        for (WorkerEndpoint n : neighbors) {
            neighbourIds.add(n.getWorkerId());
        }
        neighbourIds.sort(Integer::compareTo);
        GetCoordinatorStateResponse.Builder response = GetCoordinatorStateResponse.newBuilder()
                .setCurrentTerm(getCurrentTerm())
                .setLeaderId(leaderId() == null ? -1 : leaderId())
                .setIsLeader(isLeader())
                .setCommitProgressIndex(store.progress())
                .setWorkerId(workerId)
                .setJac(jacNow)
                .setJobsInCurrentTerm(termCountNow);
        synchronized (stateLock) {
            leaderJacNow = leaderJac;
        }
        response.setLeaderJac(leaderJacNow);
        for (int id : neighbourIds) {
            response.addNeighborIds(id);
        }
        for (ReplicatedJobStore.JobRecord job : store.snapshots()) {
            response.addJobs(CoordinatorJobMapper.toJobProto(job));
        }
        return response.build();
    }

    /** Suggested §5 Ping: liveness probe for peers and operators. */
    com.cs324a2.grpc.coordinator.PingResponse handlePing(int callerId) {
        return com.cs324a2.grpc.coordinator.PingResponse.newBuilder()
                .setWorkerId(workerId).setIsLeader(isLeader())
                .setTerm(getCurrentTerm()).build();
    }

    /**
     * Server-streaming progress for the client GUI. Emits one
     * {@link JobProgressUpdate} per distributed-work mutation (assign, complete,
     * retry, job-done), derived from actual task checkpoints. Both leaders and
     * followers can serve it from their local replica, so progress survives
     * coordinator failover. Completes on terminal job state.
     */
    void subscribeJobProgress(JobProgressRequest request,
                              StreamObserver<JobProgressUpdate> observer) {
        if (request == null || request.getJobId() == null || request.getJobId().isBlank()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("job_id must not be blank")
                    .asRuntimeException());
            return;
        }
        String jobId = request.getJobId();
        ServerCallStreamObserver<JobProgressUpdate> serverObserver =
                (observer instanceof ServerCallStreamObserver<?> raw)
                        ? (ServerCallStreamObserver<JobProgressUpdate>) raw : null;
        long lastSentProgress = -1;
        try {
            while (!closed.get()) {
                if (Context.current().isCancelled()) return;
                if (serverObserver != null && serverObserver.isCancelled()) return;
                ReplicatedJobStore.JobRecord record = store.get(jobId);
                if (record == null) {
                    Thread.sleep(50);
                    continue;
                }
                if (record.progressIndex() != lastSentProgress) {
                    JobProgressUpdate update = toProgressUpdate(record);
                    observer.onNext(update);
                    lastSentProgress = record.progressIndex();
                    if (record.status() != ReplicatedJobStore.JobStatus.ACTIVE) {
                        observer.onCompleted();
                        return;
                    }
                } else if (record.status() != ReplicatedJobStore.JobStatus.ACTIVE) {
                    observer.onCompleted();
                    return;
                }
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) throw e;
            try {
                observer.onError(Status.INTERNAL.withCause(e)
                        .withDescription("progress stream failed")
                        .asRuntimeException());
            } catch (RuntimeException ignored) {
                // Stream already torn down (e.g. client cancelled).
            }
        }
    }

    /**
     * Builds a progress update from an immutable job snapshot. Counts come
     * straight from task checkpoints; {@code completion_fraction} is
     * {@code completed/total} (1.0 for empty jobs).
     */
    static JobProgressUpdate toProgressUpdate(ReplicatedJobStore.JobRecord snapshot) {
        List<ReplicatedJobStore.TaskRecord> tasks = snapshot.tasks();
        int total = tasks.size();
        int completed = 0;
        int running = 0;
        int pending = 0;
        int failed = 0;
        java.util.Map<Integer, int[]> perWorker = new java.util.HashMap<>();
        for (ReplicatedJobStore.TaskRecord task : tasks) {
            switch (task.status()) {
                case COMPLETED -> completed++;
                case RUNNING -> running++;
                case FAILED -> failed++;
                default -> pending++;
            }
            int[] counts = perWorker.computeIfAbsent(task.assignedWorkerId(), k -> new int[5]);
            counts[0]++;
            switch (task.status()) {
                case COMPLETED -> counts[1]++;
                case RUNNING -> counts[2]++;
                case FAILED -> counts[4]++;
                default -> counts[3]++;
            }
        }
        double fraction = total == 0 ? 1.0 : completed / (double) total;
        if (snapshot.request().operation() == ComputeOperation.PRIMESUM && total > 0) {
            java.math.BigInteger count = java.math.BigInteger.valueOf(snapshot.request().end())
                    .subtract(java.math.BigInteger.valueOf(snapshot.request().start()))
                    .add(java.math.BigInteger.ONE);
            java.math.BigInteger processed = java.math.BigInteger.ZERO;
            for (ReplicatedJobStore.TaskRecord task : tasks) {
                java.math.BigInteger[] bounds = WorkPartitioner.partitionBounds(
                        snapshot.request(), total, task.unit().partitionId());
                if (task.status() == ReplicatedJobStore.TaskState.COMPLETED) {
                    processed = processed.add(bounds[1].subtract(bounds[0])
                            .add(java.math.BigInteger.ONE));
                } else {
                    processed = processed.add(java.math.BigInteger.valueOf(task.unit().start())
                            .subtract(bounds[0]));
                }
            }
            fraction = processed.doubleValue() / count.doubleValue();
        }
        JobStatus status = switch (snapshot.status()) {
            case COMPLETED -> JobStatus.JOB_COMPLETED;
            case FAILED -> JobStatus.JOB_FAILED;
            default -> JobStatus.JOB_ACTIVE;
        };
        JobProgressUpdate.Builder update = JobProgressUpdate.newBuilder()
                .setJobId(snapshot.jobId())
                .setTotalTasks(total)
                .setCompletedTasks(completed)
                .setRunningTasks(running)
                .setPendingTasks(pending)
                .setFailedTasks(failed)
                .setCompletionFraction(fraction)
                .setProgressIndex(snapshot.progressIndex())
                .setStatus(status)
                .setJobState(CoordinatorJobMapper.toJobProto(snapshot));
        if (snapshot.resultValue() != null) {
            update.setResultValue(snapshot.resultValue()).setHasResult(true);
        }
        if (!snapshot.exactResultValue().isEmpty()) {
            update.setExactResultValue(snapshot.exactResultValue()).setHasResult(true);
        }
        if (snapshot.error() != null) update.setError(snapshot.error());
        perWorker.entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(entry -> update.addWorkerBreakdown(WorkerProgress.newBuilder()
                        .setWorkerId(entry.getKey())
                        .setTotalTasks(entry.getValue()[0])
                        .setCompletedTasks(entry.getValue()[1])
                        .setRunningTasks(entry.getValue()[2])
                        .setPendingTasks(entry.getValue()[3])
                        .setFailedTasks(entry.getValue()[4])
                        .build()));
        return update.build();
    }

    // ---- Election and heartbeats ----

    private void tick() {
        if (closed.get()) return;
        try {
            if (isLeader()) {
                if (System.nanoTime() - lastHeartbeatSentNanos
                        >= TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_INTERVAL_MILLIS)) {
                    broadcastHeartbeats();
                }
            } else if (getCurrentTerm() > 0 && System.nanoTime() - lastHeartbeatNanos
                    >= TimeUnit.MILLISECONDS.toNanos(electionTimeoutMillis)) {
                // Do not elect during idle startup. The first job starts the
                // election; timeouts only recover an established term.
                long silentMs = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - lastHeartbeatNanos);
                System.out.printf("[Coordinator %d] no heartbeat from leader %s for %dms"
                                + " (timeout %dms) — starting election%n",
                        workerId, leaderId(), silentMs, electionTimeoutMillis);
                initiateElectionAsync();
            }
        } catch (Throwable e) {
            System.err.printf("[Coordinator %d] timer tick failed: %s%n", workerId, e);
        }
    }

    /** Async wrapper so the timer thread never blocks on flood RPCs. */
    void initiateElectionAsync() {
        if (closed.get() || !electionInProgress.compareAndSet(false, true)) return;
        jobs.execute(() -> {
            try {
                initiateElection();
            } catch (RuntimeException e) {
                System.err.printf("[Coordinator %d] background election failed: %s%n",
                        workerId, e.getMessage());
            } finally {
                electionInProgress.set(false);
            }
        });
    }

    private void runElection() {
        long term;
        long lastProgress;
        List<WorkerEndpoint> voters;
        synchronized (stateLock) {
            if (isLeader || closed.get()) return;
            currentTerm++;
            term = currentTerm;
            votedFor = workerId;
            leaderId = null;
            isLeader = false;
            lastProgress = store.progress();
            electionCount.incrementAndGet();
        }
        voters = membership.snapshot().workers();
        System.out.printf("[Coordinator %d] starting election for term %d (members=%d progress=%d)%n",
                workerId, term, Math.max(1, voters.size()), lastProgress);
        int votes = 1; // self-vote
        int quorum = quorumOf(voters);
        for (WorkerEndpoint voter : voters) {
            if (voter.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.requestVote(voter.getHost(), voter.getPort(),
                        com.cs324a2.grpc.coordinator.RequestVoteRequest.newBuilder()
                                .setTerm(term).setCandidateId(workerId)
                                .setLastProgressIndex(lastProgress).build(),
                        RPC_TIMEOUT_MILLIS);
                if (response.getTerm() > term) {
                    synchronized (stateLock) {
                        stepDownLocked(response.getTerm());
                    }
                    System.out.printf("[Coordinator %d] stepped down: saw term %d%n",
                            workerId, response.getTerm());
                    return;
                }
                if (response.getVoteGranted() && response.getTerm() == term) votes++;
            } catch (RuntimeException e) {
                // Unreachable voter counts as a withheld vote.
            }
        }
        synchronized (stateLock) {
            if (term != currentTerm || isLeader || closed.get()) return;
            if (votes < quorum) {
                System.out.printf("[Coordinator %d] lost election for term %d (%d/%d votes)%n",
                        workerId, term, votes, quorum);
                lastHeartbeatNanos = System.nanoTime();
                return;
            }
            isLeader = true;
            leaderId = workerId;
            lastHeartbeatNanos = System.nanoTime();
        }
        lastHeartbeatSentNanos = 0; // send immediately
        System.out.printf("[Coordinator %d] *** ELECTED LEADER term=%d votes=%d/%d progress=%d ***%n",
                workerId, term, votes, quorum, store.progress());
        broadcastHeartbeats();
        recoverUnfinishedJobs();
    }

    private void broadcastHeartbeats() {
        long term;
        List<WorkerEndpoint> peers;
        synchronized (stateLock) {
            if (!isLeader || closed.get()) return;
            term = currentTerm;
            peers = membership.snapshot().workers();
        }
        lastHeartbeatSentNanos = System.nanoTime();
        int jacNow = jac.get();
        for (WorkerEndpoint peer : peers) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.heartbeat(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest.newBuilder()
                                .setTerm(term).setLeaderId(workerId)
                                .setLeaderHost(host).setLeaderPort(port)
                                .setCommitProgressIndex(store.progress())
                                .setLeaderJac(jacNow).build(),
                        RPC_TIMEOUT_MILLIS);
                recordNeighborSuccess(peer.getWorkerId());
                if (response.getTerm() > term) {
                    synchronized (stateLock) {
                        stepDownLocked(response.getTerm());
                    }
                    System.out.printf("[Coordinator %d] leader stepped down: follower saw term %d%n",
                            workerId, response.getTerm());
                    return;
                }
                if (!response.getSuccess() && response.getTerm() == term) {
                    System.err.printf("[Coordinator %d] heartbeat rejected by %d"
                                    + " (possible second leader for term %d)%n",
                            workerId, peer.getWorkerId(), term);
                }
            } catch (RuntimeException e) {
                // A departed follower misses this round; expiry is membership-driven.
                recordNeighborFailure(peer.getWorkerId());
            }
        }
    }

    // ---- Unstructured JAC election (ELECTION/COORDINATOR flood over gRPC) ----

    /** Best-candidate comparison: lowest JAC wins; ties break to highest ID. */
    static boolean isBetterCandidate(int idA, int jacA, int idB, int jacB) {
        if (jacA != jacB) return jacA < jacB;
        return idA > idB;
    }

    static boolean isBetterCandidate(int idA, int jacA, long progressA,
                                     int idB, int jacB, long progressB) {
        if (progressA != progressB) return progressA > progressB;
        return isBetterCandidate(idA, jacA, idB, jacB);
    }

    public int getJAC() {
        return jac.get();
    }

    public void setJAC(int value) {
        jac.set(value);
    }

    public int getJobsInCurrentTerm() {
        return jobsInCurrentTerm.get();
    }

    public List<WorkerEndpoint> getNeighbors() {
        return List.copyOf(neighbors);
    }

    /** Adds a neighbour link; ignores self and duplicates. Returns true if added. */
    public boolean addNeighborEndpoint(WorkerEndpoint endpoint) {
        if (endpoint == null || endpoint.getWorkerId() == workerId) return false;
        for (WorkerEndpoint existing : neighbors) {
            if (existing.getWorkerId() == endpoint.getWorkerId()) return false;
        }
        neighbors.add(endpoint);
        System.out.printf("[Coordinator %d] neighbours: worker %d linked to worker %d"
                        + " at %s:%d — neighbours now %s%n",
                workerId, workerId, endpoint.getWorkerId(),
                endpoint.getHost(), endpoint.getPort(), neighborIdsString());
        return true;
    }

    /** Sorted neighbour ID list for terminal output. */
    private String neighborIdsString() {
        List<Integer> ids = new ArrayList<>();
        for (WorkerEndpoint n : neighbors) {
            ids.add(n.getWorkerId());
        }
        ids.sort(Integer::compareTo);
        return ids.toString();
    }

    public boolean handleAddNeighbor(int neighbourId, String neighbourHost, int neighbourPort) {
        if (neighbourId <= 0 || neighbourHost == null || neighbourHost.isBlank()
                || neighbourPort < 1 || neighbourPort > 65535
                || neighbourId == workerId) {
            return false;
        }
        return addNeighborEndpoint(WorkerEndpoint.newBuilder()
                .setWorkerId(neighbourId).setHost(neighbourHost).setPort(neighbourPort)
                .setRegistrationId("neighbour-" + neighbourId).build());
    }

    /**
     * Connects to one random active worker from Bootstrap and forms a bilateral
     * neighbour link via gRPC. No-op when alone or already meshed.
     */
    public void connectToRandomNeighbor() {
        List<WorkerEndpoint> actives;
        try {
            actives = membership.snapshot().workers().stream()
                    .filter(w -> w.getWorkerId() != workerId).toList();
        } catch (RuntimeException e) {
            return;
        }
        if (actives.isEmpty()) return;
        List<WorkerEndpoint> unknown = actives.stream()
                .filter(w -> neighbors.stream()
                        .noneMatch(n -> n.getWorkerId() == w.getWorkerId()))
                .toList();
        if (unknown.isEmpty()) return;
        WorkerEndpoint peer = unknown.get(
                ThreadLocalRandom.current().nextInt(unknown.size()));
        System.out.printf("[Coordinator %d] joining: randomly chosen active worker is %d"
                        + " at %s:%d — requesting bilateral neighbour link%n",
                workerId, peer.getWorkerId(), peer.getHost(), peer.getPort());
        addNeighborEndpoint(peer);
        try {
            var response = coordClient.addNeighbor(peer.getHost(), peer.getPort(),
                    com.cs324a2.grpc.coordinator.NeighborRequest.newBuilder()
                            .setWorkerId(workerId).setHost(host).setPort(port).build(),
                    RPC_TIMEOUT_MILLIS);
            System.out.printf("[Coordinator %d] neighbour link with worker %d %s"
                            + " — neighbours now %s%n",
                    workerId, peer.getWorkerId(),
                    response.getAdded() ? "confirmed bilateral" : "already existed",
                    neighborIdsString());
        } catch (RuntimeException e) {
            System.err.printf("[Coordinator %d] random link to %d failed: %s%n",
                    workerId, peer.getWorkerId(), e.getMessage());
        }
    }

    /** Resolves flood peers: neighbours when linked, else full membership. */
    private List<WorkerEndpoint> floodPeersExcluding(int senderId) {
        List<WorkerEndpoint> base = neighbors.isEmpty()
                ? membership.snapshot().workers()
                : List.copyOf(neighbors);
        return base.stream().filter(w -> w.getWorkerId() != workerId
                && w.getWorkerId() != senderId).toList();
    }

    private boolean hasLiveLeader() {
        Integer id;
        synchronized (stateLock) {
            id = leaderId;
            if (isLeader) return true;
            if (id == null) return false;
        }
        long sinceHeartbeat = System.nanoTime() - lastHeartbeatNanos;
        return sinceHeartbeat < TimeUnit.MILLISECONDS.toNanos(electionTimeoutMillis);
    }

    /**
     * ELECTION convergecast handler. Duplicate election_ids are ignored
     * (duplicate=true) so each worker processes a given ELECTION once; the
     * caller ignores those subtrees. Otherwise floods to neighbours except the
     * sender and returns the best reachable candidate.
     */
    public com.cs324a2.grpc.coordinator.ElectionResponse handlePropagateElection(
            com.cs324a2.grpc.coordinator.ElectionRequest request) {
        String electionId = request.getElectionId();
        if (!seenElectionIds.add(electionId)) {
            return com.cs324a2.grpc.coordinator.ElectionResponse.newBuilder()
                    .setDuplicate(true).setTerm(getCurrentTerm()).build();
        }
        System.out.printf("[Coordinator %d] ELECTION %s from %d JAC=%d%n",
                workerId, electionId, request.getSenderId(), jac.get());
        int bestId = workerId;
        int bestJac = jac.get();
        long bestProgress = store.progress();
        String bestHost = host;
        int bestPort = port;
        for (WorkerEndpoint peer : floodPeersExcluding(request.getSenderId())) {
            try {
                var child = coordClient.propagateElection(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.ElectionRequest.newBuilder()
                                .setElectionId(electionId).setSenderId(workerId)
                                .setSenderHost(host).setSenderPort(port).build(),
                        RPC_TIMEOUT_MILLIS + 1_200);
                recordNeighborSuccess(peer.getWorkerId());
                if (child.getDuplicate()) continue;
                if (isBetterCandidate(child.getBestWorkerId(), child.getBestJac(),
                        child.getBestProgressIndex(), bestId, bestJac, bestProgress)) {
                    bestId = child.getBestWorkerId();
                    bestJac = child.getBestJac();
                    bestProgress = child.getBestProgressIndex();
                    bestHost = child.getBestHost();
                    bestPort = child.getBestPort();
                }
            } catch (RuntimeException e) {
                System.err.printf("[Coordinator %d] ELECTION %s child %d failed: %s%n",
                        workerId, electionId, peer.getWorkerId(), e.getMessage());
                recordNeighborFailure(peer.getWorkerId());
            }
        }
        System.out.printf("[Coordinator %d] subtree best for %s = id=%d JAC=%d%n",
                workerId, electionId, bestId, bestJac);
        return com.cs324a2.grpc.coordinator.ElectionResponse.newBuilder()
                .setDuplicate(false).setBestWorkerId(bestId).setBestJac(bestJac)
                .setBestHost(bestHost).setBestPort(bestPort)
                .setBestProgressIndex(bestProgress)
                .setTerm(getCurrentTerm()).build();
    }

    /**
     * COORDINATOR flood handler. Duplicate message_ids are ignored; otherwise
     * installs the announced leader (agreement) and forwards to neighbours
     * except the sender so all reachable workers learn the result. Stale terms
     * are rejected; same-term conflicts resolve deterministically to the
     * better JAC candidate so concurrent elections still converge.
     */
    public com.cs324a2.grpc.coordinator.CoordinatorAck handlePropagateCoordinator(
            com.cs324a2.grpc.coordinator.CoordinatorAnnounce request) {
        if (!seenCoordinatorIds.add(request.getCoordinatorMessageId())) {
            return com.cs324a2.grpc.coordinator.CoordinatorAck.newBuilder()
                    .setAccepted(false).setTerm(getCurrentTerm()).build();
        }
        long seenTerm = getCurrentTerm();
        if (request.getTerm() < seenTerm) {
            return com.cs324a2.grpc.coordinator.CoordinatorAck.newBuilder()
                    .setAccepted(false).setTerm(seenTerm).build();
        }
        synchronized (stateLock) {
            if (request.getTerm() == currentTerm && leaderId != null
                    && !leaderId.equals(request.getCoordinatorId())
                    && !isBetterCandidate(request.getCoordinatorId(),
                            request.getCoordinatorJac(), request.getCoordinatorProgressIndex(),
                            leaderId, leaderJac, leaderProgressIndex)) {
                // Keep the better same-term leader, but still forward the
                // announcement so the flood reaches the whole overlay.
                System.out.printf("[Coordinator %d] COORDINATOR %s keeps %d over %d (same term %d)%n",
                        workerId, request.getCoordinatorMessageId(),
                        leaderId, request.getCoordinatorId(), currentTerm);
            } else {
                System.out.printf("[Coordinator %d] COORDINATOR %s: worker %d announced as"
                                + " coordinator (JAC=%d) for term %d, forwarded by worker %d%n",
                        workerId, request.getCoordinatorMessageId(),
                        request.getCoordinatorId(), request.getCoordinatorJac(),
                        request.getTerm(), request.getSenderId());
                installCoordinatorStateLocked(request.getCoordinatorId(),
                        request.getCoordinatorHost(), request.getCoordinatorPort(),
                        request.getTerm(), request.getCoordinatorJac(),
                        request.getCoordinatorProgressIndex());
                if (request.getCoordinatorId() != workerId) {
                    System.out.printf("[Coordinator %d] recognizes worker %d as coordinator"
                                    + " for term %d (this node is a follower)%n",
                            workerId, request.getCoordinatorId(), request.getTerm());
                }
            }
        }
        for (WorkerEndpoint peer : floodPeersExcluding(request.getSenderId())) {
            try {
                coordClient.propagateCoordinator(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.CoordinatorAnnounce.newBuilder(
                                request).setSenderId(workerId)
                                .setSenderHost(host).setSenderPort(port).build(),
                        RPC_TIMEOUT_MILLIS);
                recordNeighborSuccess(peer.getWorkerId());
            } catch (RuntimeException e) {
                System.err.printf("[Coordinator %d] COORDINATOR forward to %d failed: %s%n",
                        workerId, peer.getWorkerId(), e.getMessage());
                recordNeighborFailure(peer.getWorkerId());
            }
        }
        return com.cs324a2.grpc.coordinator.CoordinatorAck.newBuilder()
                .setAccepted(true).setTerm(getCurrentTerm()).build();
    }

    private void installCoordinatorState(int electedId, String electedHost,
                                         int electedPort, long term) {
        installCoordinatorStateUnsync(electedId, electedHost, electedPort, term,
                Integer.MAX_VALUE, store.progress());
    }

    private void installCoordinatorState(int electedId, String electedHost,
                                         int electedPort, long term, int electedJac) {
        installCoordinatorStateUnsync(electedId, electedHost, electedPort,
                term, electedJac, store.progress());
    }

    /** Caller must hold {@code stateLock} when calling the locked variant. */
    private void installCoordinatorStateLocked(int electedId, String electedHost,
                                               int electedPort, long term, int electedJac,
                                               long electedProgress) {
        boolean was = isLeader;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        leaderId = electedId;
        leaderJac = electedJac;
        leaderProgressIndex = electedProgress;
        isLeader = (electedId == workerId);
        leaderReady = !isLeader || (was && leaderReady);
        lastHeartbeatNanos = System.nanoTime();
        boolean became = isLeader && !was;
        synchronized (termStateLock) {
            inFlightJobs = 0;
            termClosing = false;
            termTransitionStarted = false;
            if (isLeader) jobsInCurrentTerm.set(0);
        }
        WorkerEndpoint elected = WorkerEndpoint.newBuilder().setWorkerId(electedId)
                .setHost(electedHost == null || electedHost.isBlank() ? "localhost" : electedHost)
                .setPort(electedPort > 0 ? electedPort : port)
                .setRegistrationId("elected-" + electedId).build();
        boolean becameCopy = became;
        boolean leaderCopy = isLeader;
        if (electedId != workerId) addNeighborEndpoint(elected);
        // RPCs outside the state lock.
        if (becameCopy || leaderCopy) {
            lastHeartbeatSentNanos = 0;
        }
        if (becameCopy) {
            System.out.printf("[Coordinator %d] *** worker %d BECAME COORDINATOR *** term=%d"
                            + " JAC=%d (announcing to all reachable workers)%n",
                    workerId, workerId, currentTerm, jac.get());
        }
        if (becameCopy || leaderCopy) {
            // Release stateLock before network I/O: copy what we need.
            // (Called with stateLock held; use a detached task to avoid nesting.)
            try {
                jobs.execute(() -> {
                    try {
                        broadcastHeartbeats();
                    } catch (RuntimeException ignored) {
                        // Best effort; the timer repeats heartbeats.
                    }
                    if (leaderCopy) {
                        try {
                            pullCoordinatorState();
                            synchronized (stateLock) {
                                if (isLeader && currentTerm == term) leaderReady = true;
                            }
                            if (becameCopy && isLeader()) recoverUnfinishedJobs();
                        } catch (RuntimeException ignored) {
                            // Best effort recovery; jobs retry on demand.
                        }
                    }
                });
            } catch (RuntimeException ignored) {
                // Executor shutting down; heartbeats resume on next tick.
            }
        }
    }

    private void pullCoordinatorState() {
        for (WorkerEndpoint peer : membership.snapshot().workers()) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                GetCoordinatorStateResponse state = coordClient.getState(
                        peer.getHost(), peer.getPort(), RPC_TIMEOUT_MILLIS);
                for (JobStateProto job : state.getJobsList()) {
                    store.applyReplicated(job, job.getProgressIndex());
                }
            } catch (RuntimeException ignored) {
                // A reachable replica is enough; unavailable peers can rejoin later.
            }
        }
    }

    private void installCoordinatorStateUnsync(int electedId, String electedHost,
                                               int electedPort, long term, int electedJac,
                                               long electedProgress) {
        synchronized (stateLock) {
            installCoordinatorStateLocked(electedId, electedHost, electedPort,
                    term, electedJac, electedProgress);
        }
    }

    /**
     * Initiates a JAC flood election when no live coordinator holds the term.
     * Any worker may call this; floods ELECTION, picks lowest-JAC (ties to
     * highest ID), installs locally, then floods COORDINATOR.
     */
    public void initiateElection() {
        synchronized (electionLock) {
            if (hasLiveLeader()) {
                System.out.printf("[Coordinator %d] election aborted: leader %s still active%n",
                        workerId, leaderId());
                return;
            }
            // A dead hint must not block the new term.
            synchronized (stateLock) {
                if (leaderId != null) {
                    long sinceHeartbeat = System.nanoTime() - lastHeartbeatNanos;
                    if (sinceHeartbeat >= TimeUnit.MILLISECONDS.toNanos(electionTimeoutMillis)) {
                        leaderId = null;
                        leaderJac = Integer.MAX_VALUE;
                        isLeader = false;
                    }
                }
            }
            long term;
            synchronized (stateLock) {
                currentTerm++;
                term = currentTerm;
            }
            electionCount.incrementAndGet();
            String electionId = "ELECTION-" + workerId + "-" + electionSeq.incrementAndGet();
            System.out.printf("%n[Coordinator %d] ========== worker %d INITIATED ELECTION %s"
                            + " for term %d (own JAC=%d, flooding %d peer(s)) ==========%n",
                    workerId, workerId, electionId, term, jac.get(),
                    floodPeersExcluding(-1).size());
            seenElectionIds.add(electionId);

            int bestId = workerId;
            int bestJac = jac.get();
            long bestProgress = store.progress();
            String bestHost = host;
            int bestPort = port;
            for (WorkerEndpoint peer : floodPeersExcluding(-1)) {
                try {
                    var child = coordClient.propagateElection(peer.getHost(), peer.getPort(),
                            com.cs324a2.grpc.coordinator.ElectionRequest.newBuilder()
                                    .setElectionId(electionId).setSenderId(workerId)
                                    .setSenderHost(host).setSenderPort(port).build(),
                            RPC_TIMEOUT_MILLIS + 1_200);
                    recordNeighborSuccess(peer.getWorkerId());
                    if (child.getDuplicate()) continue;
                    if (isBetterCandidate(child.getBestWorkerId(), child.getBestJac(),
                            child.getBestProgressIndex(), bestId, bestJac, bestProgress)) {
                        bestId = child.getBestWorkerId();
                        bestJac = child.getBestJac();
                        bestProgress = child.getBestProgressIndex();
                        bestHost = child.getBestHost();
                        bestPort = child.getBestPort();
                    }
                    if (child.getTerm() > term) {
                        synchronized (stateLock) {
                            if (child.getTerm() > currentTerm) {
                                stepDownLocked(child.getTerm());
                            }
                        }
                        term = getCurrentTerm();
                    }
                } catch (RuntimeException e) {
                    System.err.printf("[Coordinator %d] election child %d failed: %s%n",
                            workerId, peer.getWorkerId(), e.getMessage());
                    recordNeighborFailure(peer.getWorkerId());
                }
            }
            System.out.printf("[Coordinator %d] ELECTION %s complete: elected worker %d"
                            + " as coordinator (JAC=%d) for term %d — flooding COORDINATOR%n",
                    workerId, electionId, bestId, bestJac, term);
            installCoordinatorStateUnsync(bestId, bestHost, bestPort, term,
                    bestJac, bestProgress);

            String coordMsgId = "COORDINATOR-" + electionId;
            seenCoordinatorIds.add(coordMsgId);
            var announce = com.cs324a2.grpc.coordinator.CoordinatorAnnounce.newBuilder()
                    .setCoordinatorMessageId(coordMsgId).setCoordinatorId(bestId)
                    .setCoordinatorHost(bestHost).setCoordinatorPort(bestPort)
                    .setCoordinatorJac(bestJac).setCoordinatorProgressIndex(bestProgress)
                    .setTerm(term)
                    .setSenderId(workerId).setSenderHost(host).setSenderPort(port).build();
            for (WorkerEndpoint peer : floodPeersExcluding(-1)) {
                try {
                    coordClient.propagateCoordinator(peer.getHost(), peer.getPort(),
                            announce, RPC_TIMEOUT_MILLIS);
                    recordNeighborSuccess(peer.getWorkerId());
                } catch (RuntimeException e) {
                    System.err.printf("[Coordinator %d] coordinator broadcast to %d failed: %s%n",
                            workerId, peer.getWorkerId(), e.getMessage());
                    recordNeighborFailure(peer.getWorkerId());
                }
            }
            System.out.printf("[Coordinator %d] ========== ELECTION %s COMPLETE: coordinator=%d"
                            + " term=%d ==========%n%n",
                    workerId, electionId, bestId, term);
        }
    }

    // ---- Job execution with checkpoint replication ----

    private long executeNewJob(JobRequest request) throws JobExecutionException {
        // Idempotent admission: a retried client or a resumed job reuses checkpoints.
        ReplicatedJobStore.JobRecord existing = store.get(request.jobId());
        if (existing != null) {
            if (existing.status() == ReplicatedJobStore.JobStatus.COMPLETED) {
                return existing.resultValue();
            }
            if (existing.status() == ReplicatedJobStore.JobStatus.FAILED) {
                throw new JobExecutionException("job " + request.jobId()
                        + " already failed: " + existing.error());
            }
            return resumeJob(request.jobId());
        }
        // One-term / five-job admission: increments JAC + term count.
        JobAdmission admission = admitTopLevelJob();
        try {
            confirmLeadership();
            long term = getCurrentTerm();
            List<WorkerEndpoint> members = membership.snapshot().workers();
            int partitions = partitionCountFor(request, members.size());
            List<WorkUnit> units = WorkPartitioner.partition(request, partitions);
            ReplicatedJobStore.JobRecord admitted;
            if (units.isEmpty()) {
                admitted = store.createEmptyJob(request, term, 0L);
                System.out.printf("[Coordinator %d] event=ADMIT progress=%d jobId=%s term=%d (empty) JAC=%d termCount=%d/%d%n",
                        workerId, admitted.progressIndex(), request.jobId(), term,
                        admission.jac(), admission.termCount(), MAX_JOBS_PER_TERM);
                replicate(admitted.snapshot());
                return 0L;
            }
            admitted = store.createJob(request, units, term);
            System.out.printf("[Coordinator %d] event=ADMIT progress=%d jobId=%s term=%d tasks=%d JAC=%d termCount=%d/%d%n",
                    workerId, admitted.progressIndex(), request.jobId(), term, units.size(),
                    admission.jac(), admission.termCount(), MAX_JOBS_PER_TERM);
            replicate(admitted.snapshot());
            return runDispatchLoop(request.jobId());
        } finally {
            completeTopLevelJob(admission);
        }
    }

    /** Admission record for the five-jobs-per-term rule. */
    private record JobAdmission(long termSequence, int jac, int termCount) {
    }

    private JobAdmission admitTopLevelJob() throws JobExecutionException {
        synchronized (stateLock) {
            if (!isLeader) {
                throw new NotLeaderException("not the leader",
                        leaderId, null, -1);
            }
            synchronized (termStateLock) {
                if (termClosing || jobsInCurrentTerm.get() >= MAX_JOBS_PER_TERM) {
                    throw new JobExecutionException("Coordinator term is transitioning;"
                            + " try the new coordinator");
                }
                int newJac = jac.incrementAndGet();
                int termCount = jobsInCurrentTerm.incrementAndGet();
                inFlightJobs++;
                if (termCount >= MAX_JOBS_PER_TERM) {
                    termClosing = true;
                }
                return new JobAdmission(currentTerm, newJac, termCount);
            }
        }
    }

    private void completeTopLevelJob(JobAdmission admission) {
        boolean startReelection = false;
        synchronized (stateLock) {
            synchronized (termStateLock) {
                if (admission == null) return;
                if (inFlightJobs > 0) inFlightJobs--;
                // Finish-then-step-down: only when every admitted job finished.
                if (inFlightJobs == 0 && termClosing
                        && !termTransitionStarted && isLeader) {
                    termTransitionStarted = true;
                    isLeader = false;
                    leaderId = null;
                    leaderJac = Integer.MAX_VALUE;
                    jobsInCurrentTerm.set(0);
                    startReelection = true;
                }
            }
        }
        if (startReelection) {
            System.out.printf("[Coordinator %d] term closed after %d admissions;"
                            + " in-flight drained, initiating new election%n",
                    workerId, MAX_JOBS_PER_TERM);
            initiateElectionAsync();
        }
    }

    /**
     * How many tasks a job is split into.
     *
     * <p>List jobs use one partition per worker. Range jobs are split into
     * {@link #RANGE_PARTITIONS_PER_WORKER} partitions per worker so that a long
     * sieve reports many completed sub-ranges: with only one task per worker the
     * client progress bar could sit at 0% for the whole job and then jump
     * straight to 100%, which is indistinguishable from a stuck UI. The total is
     * capped by {@link #MAX_PARTITIONS_PER_JOB} because every task mutation
     * replicates a full job snapshot to all followers, so replication cost grows
     * with (tasks x mutations).
     */
    static int partitionCountFor(JobRequest request, int memberCount) {
        int members = Math.max(1, memberCount);
        if (request.operation() != ComputeOperation.PRIMESUM) {
            return members;
        }
        long desired = (long) members * RANGE_PARTITIONS_PER_WORKER;
        return (int) Math.min(desired, Math.max(1, MAX_PARTITIONS_PER_JOB));
    }

    private long resumeJob(String jobId) throws JobExecutionException {
        if (runningJobs.putIfAbsent(jobId, Boolean.TRUE) != null) {
            // Another thread on this leader is already driving it; wait for it.
            return awaitRunningJob(jobId);
        }
        try {
            return runDispatchLoop(jobId);
        } finally {
            runningJobs.remove(jobId);
        }
    }

    private long awaitRunningJob(String jobId) throws JobExecutionException {
        long deadline = System.nanoTime() + config.jobTimeout().toNanos();
        boolean unbounded = store.get(jobId).request().operation() == ComputeOperation.PRIMESUM;
        while (unbounded || System.nanoTime() < deadline) {
            ReplicatedJobStore.JobRecord record = store.get(jobId);
            if (record != null) {
                if (record.status() == ReplicatedJobStore.JobStatus.COMPLETED) {
                    return record.resultValue();
                }
                if (record.status() == ReplicatedJobStore.JobStatus.FAILED) {
                    throw new JobExecutionException("job " + jobId + " failed: "
                            + record.error());
                }
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JobExecutionException("interrupted while awaiting job " + jobId, e);
            }
        }
        throw new JobExecutionException("timed out awaiting in-progress job " + jobId);
    }

    private void recoverUnfinishedJobs() {
        List<ReplicatedJobStore.JobRecord> unfinished = store.activeSnapshots();
        for (ReplicatedJobStore.JobRecord job : unfinished) {
            long done = job.tasks().stream()
                    .filter(t -> t.status() == ReplicatedJobStore.TaskState.COMPLETED).count();
            System.out.printf("[Coordinator %d] event=RECOVER progress=%d jobId=%s"
                            + " completedTasks=%d/%d term=%d%n",
                    workerId, store.progress(), job.jobId(), done,
                    job.tasks().size(), getCurrentTerm());
            if (runningJobs.putIfAbsent(job.jobId(), Boolean.TRUE) == null) {
                jobs.execute(() -> {
                    try {
                        runDispatchLoop(job.jobId());
                    } catch (JobExecutionException e) {
                        System.err.printf("[Coordinator %d] recovery of %s ended: %s%n",
                                workerId, job.jobId(), e.getMessage());
                    } finally {
                        runningJobs.remove(job.jobId());
                    }
                });
            }
        }
    }

    private long runDispatchLoop(String jobId) throws JobExecutionException {
        boolean owner = runningJobs.putIfAbsent(jobId, Boolean.TRUE) == null;
        long jobDeadline = System.nanoTime() + config.jobTimeout().toNanos();
        try {
            reclaimStaleRunningTasks(jobId);
            while (true) {
                ensureLeader(jobId);
                if (System.nanoTime() >= jobDeadline
                        && store.live(jobId).request().operation() != ComputeOperation.PRIMESUM) {
                    return failJob(jobId, "job exceeded its deadline");
                }
                ReplicatedJobStore.JobRecord live = store.live(jobId);
                if (live == null) throw new JobExecutionException("job " + jobId + " vanished");
                List<ReplicatedJobStore.TaskRecord> snapshot;
                synchronized (live) {
                    snapshot = live.tasks();
                }
                if (snapshot.stream().allMatch(
                        t -> t.status() == ReplicatedJobStore.TaskState.COMPLETED)) {
                    return finishJob(jobId);
                }
                for (ReplicatedJobStore.TaskRecord task : snapshot) {
                    if (task.status() == ReplicatedJobStore.TaskState.FAILED) {
                        return failJob(jobId, task.lastFailure());
                    }
                }
                WorkerMembership.Snapshot members = membership.snapshot();
                taskClient.retainWorkers(members.version(), members.workers());
                List<WorkerEndpoint> workers = members.workers().stream()
                        .filter(com.cs324a2.client.CoordinatorGrpcClient::isReachable)
                        .filter(w -> store.live(jobId).request().operation() == ComputeOperation.PRIMESUM
                                || dispatchFailures.getOrDefault(w.getRegistrationId(),
                                new AtomicInteger()).get() < 2)
                        .sorted(Comparator.comparingInt(WorkerEndpoint::getWorkerId))
                        .toList();
                if (workers.isEmpty()) {
                    try {
                        membership.awaitChange(members.version(),
                                Duration.ofMillis(100));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new JobExecutionException("interrupted awaiting workers", e);
                    }
                    continue;
                }
                List<ReplicatedJobStore.TaskRecord> pending;
                synchronized (live) {
                    pending = live.tasks().stream()
                            .filter(t -> t.status() == ReplicatedJobStore.TaskState.PENDING)
                            .toList();
                }
                if (pending.isEmpty()) {
                    // All dispatched tasks are RUNNING; poll briefly for completions.
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new JobExecutionException("interrupted awaiting tasks", e);
                    }
                    continue;
                }
                dispatchBatch(jobId, live, pending, workers);
            }
        } finally {
            if (owner) runningJobs.remove(jobId);
        }
    }

    /**
     * Recovery fence: tasks left RUNNING by a dead leader return to PENDING so
     * the new leader re-dispatches them, while COMPLETED results are kept.
     */
    private void reclaimStaleRunningTasks(String jobId) {
        ReplicatedJobStore.JobRecord live = store.live(jobId);
        if (live == null) return;
        synchronized (live) {
            boolean reset = false;
            for (ReplicatedJobStore.TaskRecord task : live.tasks()) {
                if (task.status() == ReplicatedJobStore.TaskState.RUNNING) {
                    long progress = store.nextProgress();
                    live.tasks.put(task.unit().partitionId(),
                            new ReplicatedJobStore.TaskRecord(task.taskId(),
                                    task.unit(),
                                    ReplicatedJobStore.TaskState.PENDING,
                                    task.attempt(), task.assignedWorkerId(),
                                    null, "reclaimed from dead leader", progress,
                                    task.exactResultValue()));
                    live.progressIndex = progress;
                    reset = true;
                }
            }
            if (reset) {
                System.out.printf("[Coordinator %d] event=RESUME progress=%d jobId=%s"
                                + " (in-flight tasks reclaimed)%n",
                        workerId, live.progressIndex, jobId);
                replicate(live.snapshot());
            }
        }
    }

    private void dispatchBatch(String jobId, ReplicatedJobStore.JobRecord live,
                               List<ReplicatedJobStore.TaskRecord> pending,
                               List<WorkerEndpoint> workers) throws JobExecutionException {
        List<Future<TaskOutcome>> futures = new ArrayList<>();
        List<DispatchedTask> dispatched = new ArrayList<>();
        synchronized (live) {
            for (ReplicatedJobStore.TaskRecord task : pending) {
                ReplicatedJobStore.TaskRecord current =
                        live.tasks.get(task.unit().partitionId());
                if (current == null
                        || current.status() != ReplicatedJobStore.TaskState.PENDING) continue;
                List<WorkerEndpoint> candidates = workers;
                if (current.attempt() > 0 && workers.size() > 1) {
                    candidates = workers.stream()
                            .filter(w -> w.getWorkerId() != current.assignedWorkerId())
                            .toList();
                }
                WorkerEndpoint target = candidates.get(Math.floorMod(
                        workerCursor.getAndIncrement(), candidates.size()));
                int attempt = current.attempt() + 1;
                long progress = store.nextProgress();
                ReplicatedJobStore.TaskRecord running =
                        new ReplicatedJobStore.TaskRecord(current.taskId(),
                                current.unit(), ReplicatedJobStore.TaskState.RUNNING,
                                attempt, target.getWorkerId(), null, null, progress,
                                current.exactResultValue());
                live.tasks.put(current.unit().partitionId(), running);
                live.progressIndex = progress;
                System.out.printf("[Coordinator %d] event=ASSIGN progress=%d jobId=%s"
                                + " taskId=%s workerId=%d attempt=%d%n",
                        workerId, progress, jobId, running.taskId(),
                        target.getWorkerId(), attempt);
                var request = WorkerProtoMapper.toRequest(
                        running.taskId(), attempt, running.unit());
                var accepted = new DispatchedTask(running, target, request);
                dispatched.add(accepted);
                futures.add(jobs.submit(() -> {
                    try {
                        var response = taskClient.execute(target, request,
                                config.taskTimeout());
                        return new TaskOutcome(accepted, response, null);
                    } catch (Throwable e) {
                        return new TaskOutcome(accepted, null, e);
                    }
                }));
            }
            replicate(live.snapshot());
        }
        // Completion-driven collection: fast tasks complete, replicate, and
        // unblock observers even while a sibling task is still blocked, so a
        // mid-job kill always finds replicated checkpoints on followers.
        java.util.Map<Future<TaskOutcome>, DispatchedTask> remaining =
                new java.util.HashMap<>();
        for (int i = 0; i < futures.size(); i++) {
            remaining.put(futures.get(i), dispatched.get(i));
        }
        long batchDeadline = System.nanoTime()
                + config.taskTimeout().toNanos() + TimeUnit.SECONDS.toNanos(1);
        while (!remaining.isEmpty()) {
            if (closed.get()) {
                remaining.keySet().forEach(f -> f.cancel(true));
                throw new JobExecutionException("coordinator " + workerId
                        + " shut down during job " + jobId);
            }
            ensureLeader(jobId);
            long waitNanos = Math.max(1L, batchDeadline - System.nanoTime());
            boolean batchExpired = waitNanos <= 1L && System.nanoTime() >= batchDeadline;
            Future<TaskOutcome> done = null;
            TaskOutcome outcome = null;
            if (!batchExpired) {
                long pollMillis = Math.min(TimeUnit.NANOSECONDS.toMillis(waitNanos) + 1, 50);
                for (var entry : new ArrayList<>(remaining.entrySet())) {
                    if (entry.getKey().isDone()) {
                        done = entry.getKey();
                        break;
                    }
                }
                if (done == null) {
                    try {
                        Thread.sleep(pollMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        remaining.keySet().forEach(f -> f.cancel(true));
                        throw new JobExecutionException(
                                "interrupted during job " + jobId, e);
                    }
                    continue;
                }
                try {
                    outcome = done.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    remaining.keySet().forEach(f -> f.cancel(true));
                    throw new JobExecutionException(
                            "interrupted during job " + jobId, e);
                } catch (Exception e) {
                    outcome = new TaskOutcome(remaining.get(done), null, e);
                }
            } else {
                var entry = remaining.entrySet().iterator().next();
                done = entry.getKey();
                done.cancel(true);
                outcome = new TaskOutcome(entry.getValue(), null,
                        new TimeoutException("task exceeded " + config.taskTimeout()));
            }
            remaining.remove(done);
            if (closed.get()) {
                remaining.keySet().forEach(f -> f.cancel(true));
                throw new JobExecutionException("coordinator " + workerId
                        + " shut down during job " + jobId);
            }
            ensureLeader(jobId);
            applyOutcome(jobId, live, outcome);
        }
    }

    private void applyOutcome(String jobId, ReplicatedJobStore.JobRecord live,
                              TaskOutcome outcome) throws JobExecutionException {
        synchronized (live) {
            ReplicatedJobStore.TaskRecord current = live.tasks.get(
                    outcome.dispatched().task().unit().partitionId());
            // Fence stale completions: only the current attempt may transition.
            if (current == null || current.status()
                    != ReplicatedJobStore.TaskState.RUNNING
                    || current.attempt() != outcome.dispatched().task().attempt()) {
                System.out.printf("[Coordinator %d] event=IGNORE_STALE progress=%d"
                                + " jobId=%s taskId=%s%n",
                        workerId, store.progress(), jobId,
                        outcome.dispatched().task().taskId());
                return;
            }
            if (outcome.error() != null) {
                if (isTransportFailure(outcome.error())
                        && !(live.request().operation() == ComputeOperation.PRIMESUM
                                && isTaskTimeout(outcome.error()))) {
                    dispatchFailures.computeIfAbsent(
                            outcome.dispatched().target().getRegistrationId(),
                            ignored -> new AtomicInteger()).incrementAndGet();
                    membership.reportRpcResult(outcome.dispatched().target(), false);
                }
                String reason = outcome.error().getMessage() == null
                        ? outcome.error().getClass().getSimpleName()
                        : outcome.error().getMessage();
                if (live.request().operation() == ComputeOperation.PRIMESUM
                        && isTaskTimeout(outcome.error())
                        && current.unit().start() < current.unit().end()) {
                    long midpoint = java.math.BigInteger.valueOf(current.unit().start())
                            .add(java.math.BigInteger.valueOf(current.unit().end()))
                            .shiftRight(1).longValueExact();
                    WorkUnit smaller = WorkUnit.primeSum(jobId,
                            current.unit().partitionId(), current.unit().start(), midpoint);
                    long progress = store.nextProgress();
                    live.tasks.put(current.unit().partitionId(),
                            new ReplicatedJobStore.TaskRecord(current.taskId(), smaller,
                                    ReplicatedJobStore.TaskState.PENDING, 0,
                                    current.assignedWorkerId(), null, reason, progress,
                                    current.exactResultValue()));
                    live.progressIndex = progress;
                    replicate(live.snapshot());
                    return;
                }
                if (current.attempt() >= config.maximumAttempts()
                        && (live.request().operation() != ComputeOperation.PRIMESUM
                            || !isTransportFailure(outcome.error()))) {
                    long progress = store.nextProgress();
                    live.tasks.put(current.unit().partitionId(),
                            new ReplicatedJobStore.TaskRecord(current.taskId(),
                                    current.unit(),
                                    ReplicatedJobStore.TaskState.FAILED,
                                    current.attempt(), current.assignedWorkerId(),
                                    null, reason, progress, current.exactResultValue()));
                    live.progressIndex = progress;
                    System.out.printf("[Coordinator %d] event=FAIL progress=%d jobId=%s"
                                    + " taskId=%s detail=%s%n",
                            workerId, progress, jobId, current.taskId(), reason);
                } else {
                    long progress = store.nextProgress();
                    live.tasks.put(current.unit().partitionId(),
                            new ReplicatedJobStore.TaskRecord(current.taskId(),
                                    current.unit(),
                                    ReplicatedJobStore.TaskState.PENDING,
                                    current.attempt(), current.assignedWorkerId(),
                                    null, reason, progress, current.exactResultValue()));
                    live.progressIndex = progress;
                    System.out.printf("[Coordinator %d] event=RETRY progress=%d jobId=%s"
                                    + " taskId=%s attempt=%d detail=%s%n",
                            workerId, progress, jobId, current.taskId(),
                            current.attempt() + 1, reason);
                }
                replicate(live.snapshot());
                return;
            }
            var response = outcome.response();
            dispatchFailures.remove(outcome.dispatched().target().getRegistrationId());
            membership.reportRpcResult(outcome.dispatched().target(), true);
            boolean identity = response.getTaskId().equals(current.taskId())
                    && response.getJobId().equals(jobId)
                    && response.getPartitionId() == current.unit().partitionId()
                    && response.getWorkerId() == current.assignedWorkerId()
                    && response.getAttempt() == current.attempt();
            if (!identity) {
                long progress = store.nextProgress();
                live.tasks.put(current.unit().partitionId(),
                        new ReplicatedJobStore.TaskRecord(current.taskId(),
                                current.unit(), ReplicatedJobStore.TaskState.PENDING,
                                current.attempt(), current.assignedWorkerId(),
                                null, "mismatched result identity", progress,
                                current.exactResultValue()));
                live.progressIndex = progress;
                replicate(live.snapshot());
                return;
            }
            WorkResult result;
            try {
                result = WorkerProtoMapper.toWorkResult(response);
            } catch (IllegalArgumentException e) {
                long progress = store.nextProgress();
                live.tasks.put(current.unit().partitionId(),
                        new ReplicatedJobStore.TaskRecord(current.taskId(),
                                current.unit(), ReplicatedJobStore.TaskState.PENDING,
                                current.attempt(), current.assignedWorkerId(),
                                null, e.getMessage(), progress,
                                current.exactResultValue()));
                live.progressIndex = progress;
                replicate(live.snapshot());
                return;
            }
            long progress = store.nextProgress();
            if (live.request().operation() == ComputeOperation.PRIMESUM) {
                java.math.BigInteger previous = current.exactResultValue().isEmpty()
                        ? java.math.BigInteger.ZERO
                        : new java.math.BigInteger(current.exactResultValue());
                java.math.BigInteger sum = previous.add(result.bigValue());
                long partitionEnd = WorkPartitioner.partitionEnd(live.request(),
                        live.tasks.size(), current.unit().partitionId());
                boolean finished = current.unit().end() == partitionEnd;
                WorkUnit nextUnit = current.unit();
                if (!finished) {
                    long nextStart = current.unit().end() + 1;
                    long nextEnd = java.math.BigInteger.valueOf(nextStart)
                            .add(java.math.BigInteger.valueOf(
                                    WorkPartitioner.primeSumChunkSize(live.request().end()) - 1))
                            .min(java.math.BigInteger.valueOf(partitionEnd)).longValueExact();
                    nextUnit = WorkUnit.primeSum(jobId, current.unit().partitionId(),
                            nextStart, nextEnd);
                }
                live.tasks.put(current.unit().partitionId(),
                        new ReplicatedJobStore.TaskRecord(current.taskId(), nextUnit,
                                finished ? ReplicatedJobStore.TaskState.COMPLETED
                                        : ReplicatedJobStore.TaskState.PENDING,
                                finished ? current.attempt() : 0, current.assignedWorkerId(),
                                finished ? (sum.bitLength() <= 63 ? sum.longValue() : 0L)
                                        : null, null, progress,
                                sum.toString()));
            } else {
                live.tasks.put(current.unit().partitionId(),
                        new ReplicatedJobStore.TaskRecord(current.taskId(), current.unit(),
                                ReplicatedJobStore.TaskState.COMPLETED, current.attempt(),
                                current.assignedWorkerId(), result.value(), null, progress));
            }
            live.progressIndex = progress;
            System.out.printf("[Coordinator %d] event=COMPLETE progress=%d jobId=%s"
                            + " taskId=%s workerId=%d value=%d%n",
                    workerId, progress, jobId, current.taskId(),
                    current.assignedWorkerId(), result.value());
            replicate(live.snapshot());
        }
    }

    private static boolean isTransportFailure(Throwable error) {
        if (error instanceof TimeoutException) return true;
        if (error instanceof StatusRuntimeException status) {
            return status.getStatus().getCode() == Status.Code.UNAVAILABLE
                    || status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
        }
        return false;
    }

    private static boolean isTaskTimeout(Throwable error) {
        return error instanceof TimeoutException
                || error instanceof StatusRuntimeException status
                && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    private long finishJob(String jobId) throws JobExecutionException {
        ensureLeader(jobId);
        ReplicatedJobStore.JobRecord live = store.live(jobId);
        List<WorkUnit> units;
        List<WorkResult> results;
        synchronized (live) {
            units = live.tasks().stream().map(ReplicatedJobStore.TaskRecord::unit).toList();
            results = live.tasks().stream()
                    .map(t -> new WorkResult(jobId, t.unit().partitionId(),
                            live.request().operation(), t.resultValue(), t.exactResultValue()))
                    .toList();
        }
        long aggregate;
        try {
            if (live.request().operation() == ComputeOperation.PRIMESUM) {
                java.math.BigInteger exact = results.stream().map(WorkResult::bigValue)
                        .reduce(java.math.BigInteger.ZERO, java.math.BigInteger::add);
                synchronized (live) {
                    long progress = store.nextProgress();
                    live.status = ReplicatedJobStore.JobStatus.COMPLETED;
                    live.resultValue = exact.bitLength() <= 63 ? exact.longValue() : 0L;
                    live.exactResultValue = exact.toString();
                    live.progressIndex = progress;
                    replicate(live.snapshot());
                }
                return exact.bitLength() <= 63 ? exact.longValue() : 0L;
            }
            aggregate = ResultAggregator.aggregate(live.request(), units, results);
        } catch (IllegalArgumentException | ArithmeticException e) {
            return failJob(jobId, "aggregation failed: " + e.getMessage());
        }
        synchronized (live) {
            long progress = store.nextProgress();
            live.status = ReplicatedJobStore.JobStatus.COMPLETED;
            live.resultValue = aggregate;
            live.progressIndex = progress;
            System.out.printf("[Coordinator %d] event=JOB_DONE progress=%d jobId=%s"
                            + " result=%d%n",
                    workerId, progress, jobId, aggregate);
            replicate(live.snapshot());
        }
        return aggregate;
    }

    private long failJob(String jobId, String reason) throws JobExecutionException {
        ReplicatedJobStore.JobRecord live = store.live(jobId);
        if (live != null) {
            synchronized (live) {
                long progress = store.nextProgress();
                live.status = ReplicatedJobStore.JobStatus.FAILED;
                live.error = reason;
                live.progressIndex = progress;
                replicate(live.snapshot());
            }
        }
        throw new JobExecutionException("job " + jobId + " failed: " + reason);
    }

    private void replicate(ReplicatedJobStore.JobRecord snapshot) {
        JobStateProto proto = CoordinatorJobMapper.toJobProto(snapshot);
        long progress = snapshot.progressIndex();
        long term;
        List<WorkerEndpoint> peers;
        synchronized (stateLock) {
            if (!isLeader) return;
            term = currentTerm;
            peers = membership.snapshot().workers();
        }
        for (WorkerEndpoint peer : peers) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.replicate(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.ReplicateJobStateRequest
                                .newBuilder().setTerm(term).setLeaderId(workerId)
                                .setProgressIndex(progress).setJobState(proto).build(),
                        REPLICATE_TIMEOUT_MILLIS);
                if (response.getTerm() > term) {
                    synchronized (stateLock) {
                        stepDownLocked(response.getTerm());
                    }
                    return;
                }
            } catch (RuntimeException e) {
                System.err.printf("[Coordinator %d] replicate progress=%d to %d failed: %s%n",
                        workerId, progress, peer.getWorkerId(), e.getMessage());
            }
        }
    }

    // ---- Leadership guards (split-brain protection) ----

    private void confirmLeadership() throws JobExecutionException {
        long term;
        List<WorkerEndpoint> peers;
        synchronized (stateLock) {
            if (!isLeader) throw new NotLeaderException("not the leader",
                    leaderId, null, -1);
            term = currentTerm;
            peers = membership.snapshot().workers();
        }
        int quorum = quorumOf(peers);
        int acks = 1; // self
        int jacNow = jac.get();
        for (WorkerEndpoint peer : peers) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.heartbeat(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest
                                .newBuilder().setTerm(term).setLeaderId(workerId)
                                .setLeaderHost(host).setLeaderPort(port)
                                .setCommitProgressIndex(store.progress())
                                .setLeaderJac(jacNow).build(),
                        RPC_TIMEOUT_MILLIS);
                if (response.getTerm() > term) {
                    synchronized (stateLock) {
                        stepDownLocked(response.getTerm());
                    }
                    throw new NotLeaderException("stepped down: saw term "
                            + response.getTerm(), leaderId(), null, -1);
                }
                if (response.getSuccess()) acks++;
            } catch (StatusRuntimeException e) {
                // Unreachable peer withholds its ack.
            }
        }
        if (acks < quorum) {
            throw new JobExecutionException("leader " + workerId
                    + " lost quorum (" + acks + "/" + quorum + "); refusing job");
        }
    }

    private void ensureLeader(String jobId) throws JobExecutionException {
        if (!isLeader()) {
            throw new NotLeaderException("coordinator " + workerId
                    + " lost leadership during job " + jobId,
                    leaderId(), null, -1);
        }
    }

    private void stepDownLocked(long newTerm) {
        currentTerm = newTerm;
        votedFor = null;
        leaderId = null;
        leaderJac = Integer.MAX_VALUE;
        isLeader = false;
        leaderReady = true;
        lastHeartbeatNanos = System.nanoTime();
    }

    private static int quorumOf(List<WorkerEndpoint> voters) {
        return Math.max(1, voters.size()) / 2 + 1;
    }

    // ---- Observers ----

    public long getCurrentTerm() {
        synchronized (stateLock) {
            return currentTerm;
        }
    }

    public Integer leaderId() {
        synchronized (stateLock) {
            return leaderId;
        }
    }

    public boolean isLeader() {
        synchronized (stateLock) {
            return isLeader && leaderReady;
        }
    }

    private void awaitLocalLeaderReady() {
        if (!localLeaderInstalling()) return;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!closed.get() && !isLeader() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean localLeaderInstalling() {
        synchronized (stateLock) {
            return isLeader && !leaderReady;
        }
    }

    public long getProgress() {
        return store.progress();
    }

    public long electionCount() {
        return electionCount.get();
    }

    public ReplicatedJobStore.JobRecord job(String jobId) {
        return store.get(jobId);
    }

    public List<ReplicatedJobStore.JobRecord> jobs() {
        return store.snapshots();
    }

    private LeaderHint leaderHint() {
        Integer id;
        synchronized (stateLock) {
            id = leaderId;
        }
        if (id == null) return null;
        for (WorkerEndpoint peer : membership.snapshot().workers()) {
            if (peer.getWorkerId() == id) {
                return new LeaderHint(peer.getWorkerId(), peer.getHost(), peer.getPort());
            }
        }
        return new LeaderHint(id, null, -1);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Thread watcher = membershipWatcher;
        if (watcher != null) watcher.interrupt();
        timers.shutdownNow();
        jobs.shutdownNow();
        try {
            taskService.stopAccepting();
        } catch (RuntimeException ignored) {
            // Already stopped.
        }
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(3, TimeUnit.SECONDS)) server.shutdownNow();
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        try {
            taskService.close();
        } catch (RuntimeException ignored) {
            // Best effort.
        }
        coordClient.close();
        synchronized (stateLock) {
            isLeader = false;
        }
        System.out.printf("[Coordinator %d] shut down (was term=%d)%n",
                workerId, getCurrentTerm());
    }

    private record LeaderHint(int workerId, String host, int port) {}
    private record DispatchedTask(ReplicatedJobStore.TaskRecord task, WorkerEndpoint target,
                                  com.cs324a2.grpc.worker.ExecuteTaskRequest request) {}
    private record TaskOutcome(DispatchedTask dispatched,
                               com.cs324a2.grpc.worker.ExecuteTaskResponse response,
                               Throwable error) {}

    /** Thrown when a follower (or deposed leader) is asked to do leader work. */
    public static final class NotLeaderException extends JobExecutionException {
        private final Integer leaderId;
        private final String leaderHost;
        private final int leaderPort;

        NotLeaderException(String message, Integer leaderId,
                           String leaderHost, int leaderPort) {
            super(message);
            this.leaderId = leaderId;
            this.leaderHost = leaderHost;
            this.leaderPort = leaderPort;
        }

        public Integer leaderId() { return leaderId; }
        public String leaderHost() { return leaderHost; }
        public int leaderPort() { return leaderPort; }
    }
}
