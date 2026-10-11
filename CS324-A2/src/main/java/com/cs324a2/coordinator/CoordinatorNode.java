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
import java.util.concurrent.ConcurrentHashMap;
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
 * (terms, votes, heartbeats, replication, submit) travels over gRPC instead of
 * the legacy RMI election path.
 *
 * <p>Election is Raft-style:
 * <ul>
 *   <li>Monotonic leader {@code terms}; one vote per term per node.</li>
 *   <li>Voters prefer candidates whose replicated progress watermark is at
 *       least as advanced as their own, so the winner can restore state.</li>
 *   <li>Majority quorum of the current membership wins.</li>
 *   <li>Leaders send periodic heartbeats; followers start an election after a
 *       randomized timeout without a valid heartbeat.</li>
 *   <li>Split-brain protection: stale terms are rejected, only the first
 *       leader per term is recognised, votes are denied while a live leader
 *       holds the term, and job admission requires a majority heartbeat
 *       lease.</li>
 * </ul>
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
    private boolean isLeader;
    private volatile long lastHeartbeatNanos = System.nanoTime();
    private final long electionTimeoutMillis =
            ELECTION_TIMEOUT_BASE_MILLIS
                    + ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_JITTER_MILLIS + 1);
    private volatile long lastHeartbeatSentNanos;
    private final AtomicBoolean electionInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final ScheduledExecutorService timers;
    private final ExecutorService jobs;
    private final AtomicInteger workerCursor = new AtomicInteger();
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
        System.out.printf("[Coordinator %d] serving worker+coordinator at %s:%d (term=%d)%n",
                workerId, host, port, getCurrentTerm());
    }

    // ---- Client entry points ----

    /**
     * Submits a job. Leaders execute; followers forward once to the known
     * leader over gRPC and otherwise fail with the leader hint.
     */
    public long submitJob(JobRequest request) throws JobExecutionException {
        if (request == null) throw new IllegalArgumentException("request must not be null");
        if (closed.get()) throw new JobExecutionException("coordinator node is closed");
        if (isLeader()) {
            return executeNewJob(request);
        }
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
                // Leader hint is stale; fall through to the hinted failure below.
            }
        }
        throw new NotLeaderException("worker " + workerId + " is not the leader",
                leaderId(), hint == null ? null : hint.host(),
                hint == null ? -1 : hint.port());
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

    HeartbeatOutcome handleHeartbeat(long term, int heartbeatLeaderId,
                                     String leaderHost, int leaderPort,
                                     long commitProgress) {
        synchronized (stateLock) {
            if (term < currentTerm) return new HeartbeatOutcome(currentTerm, false);
            if (term > currentTerm) {
                stepDownLocked(term);
                leaderId = heartbeatLeaderId;
                lastHeartbeatNanos = System.nanoTime();
                store.observeProgress(commitProgress);
                return new HeartbeatOutcome(currentTerm, true);
            }
            if (leaderId == null) {
                leaderId = heartbeatLeaderId;
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
            lastHeartbeatNanos = System.nanoTime();
            store.observeProgress(commitProgress);
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
            return response.setExecuted(true).setResultValue(result).setHasResult(true)
                    .setProgressIndex(finished == null ? store.progress()
                            : finished.progressIndex())
                    .build();
        } catch (NotLeaderException e) {
            return response.setExecuted(false).build();
        } catch (JobExecutionException e) {
            ReplicatedJobStore.JobRecord failed = store.get(request.jobId());
            var builder = response.setExecuted(true)
                    .setError(e.getMessage() == null ? "job failed" : e.getMessage());
            if (failed != null) builder.setProgressIndex(failed.progressIndex());
            return builder.build();
        }
    }

    GetCoordinatorStateResponse handleGetState() {
        GetCoordinatorStateResponse.Builder response = GetCoordinatorStateResponse.newBuilder()
                .setCurrentTerm(getCurrentTerm())
                .setLeaderId(leaderId() == null ? -1 : leaderId())
                .setIsLeader(isLeader())
                .setCommitProgressIndex(store.progress())
                .setWorkerId(workerId);
        for (ReplicatedJobStore.JobRecord job : store.snapshots()) {
            response.addJobs(CoordinatorJobMapper.toJobProto(job));
        }
        return response.build();
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
            } else if (System.nanoTime() - lastHeartbeatNanos
                    >= TimeUnit.MILLISECONDS.toNanos(electionTimeoutMillis)) {
                startElection();
            }
        } catch (Throwable e) {
            System.err.printf("[Coordinator %d] timer tick failed: %s%n", workerId, e);
        }
    }

    void startElection() {
        if (closed.get() || !electionInProgress.compareAndSet(false, true)) return;
        jobs.execute(() -> {
            try {
                runElection();
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
        for (WorkerEndpoint peer : peers) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.heartbeat(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest.newBuilder()
                                .setTerm(term).setLeaderId(workerId)
                                .setLeaderHost(host).setLeaderPort(port)
                                .setCommitProgressIndex(store.progress()).build(),
                        RPC_TIMEOUT_MILLIS);
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
            }
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
        confirmLeadership();
        long term = getCurrentTerm();
        List<WorkerEndpoint> members = membership.snapshot().workers();
        int partitions = partitionCountFor(request, members.size());
        List<WorkUnit> units = WorkPartitioner.partition(request, partitions);
        ReplicatedJobStore.JobRecord admitted;
        if (units.isEmpty()) {
            admitted = store.createEmptyJob(request, term, 0L);
            System.out.printf("[Coordinator %d] event=ADMIT progress=%d jobId=%s term=%d (empty)%n",
                    workerId, admitted.progressIndex(), request.jobId(), term);
            replicate(admitted.snapshot());
            return 0L;
        }
        admitted = store.createJob(request, units, term);
        System.out.printf("[Coordinator %d] event=ADMIT progress=%d jobId=%s term=%d tasks=%d%n",
                workerId, admitted.progressIndex(), request.jobId(), term, units.size());
        replicate(admitted.snapshot());
        return runDispatchLoop(request.jobId());
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
        while (System.nanoTime() < deadline) {
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
                if (System.nanoTime() >= jobDeadline) {
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
                                    null, "reclaimed from dead leader", progress));
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
                WorkerEndpoint target = workers.get(Math.floorMod(
                        workerCursor.getAndIncrement(), workers.size()));
                int attempt = current.attempt() + 1;
                long progress = store.nextProgress();
                ReplicatedJobStore.TaskRecord running =
                        new ReplicatedJobStore.TaskRecord(current.taskId(),
                                current.unit(), ReplicatedJobStore.TaskState.RUNNING,
                                attempt, target.getWorkerId(), null, null, progress);
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
                String reason = outcome.error().getMessage() == null
                        ? outcome.error().getClass().getSimpleName()
                        : outcome.error().getMessage();
                if (current.attempt() >= config.maximumAttempts()) {
                    long progress = store.nextProgress();
                    live.tasks.put(current.unit().partitionId(),
                            new ReplicatedJobStore.TaskRecord(current.taskId(),
                                    current.unit(),
                                    ReplicatedJobStore.TaskState.FAILED,
                                    current.attempt(), current.assignedWorkerId(),
                                    null, reason, progress));
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
                                    null, reason, progress));
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
                                null, "mismatched result identity", progress));
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
                                null, e.getMessage(), progress));
                live.progressIndex = progress;
                replicate(live.snapshot());
                return;
            }
            long progress = store.nextProgress();
            live.tasks.put(current.unit().partitionId(),
                    new ReplicatedJobStore.TaskRecord(current.taskId(), current.unit(),
                            ReplicatedJobStore.TaskState.COMPLETED, current.attempt(),
                            current.assignedWorkerId(), result.value(), null, progress));
            live.progressIndex = progress;
            System.out.printf("[Coordinator %d] event=COMPLETE progress=%d jobId=%s"
                            + " taskId=%s workerId=%d value=%d%n",
                    workerId, progress, jobId, current.taskId(),
                    current.assignedWorkerId(), result.value());
            replicate(live.snapshot());
        }
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
                            live.request().operation(), t.resultValue()))
                    .toList();
        }
        long aggregate;
        try {
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
        for (WorkerEndpoint peer : peers) {
            if (peer.getWorkerId() == workerId) continue;
            try {
                var response = coordClient.heartbeat(peer.getHost(), peer.getPort(),
                        com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest
                                .newBuilder().setTerm(term).setLeaderId(workerId)
                                .setLeaderHost(host).setLeaderPort(port)
                                .setCommitProgressIndex(store.progress()).build(),
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
        isLeader = false;
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
            return isLeader;
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
