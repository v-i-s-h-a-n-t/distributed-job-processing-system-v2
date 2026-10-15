package com.cs324a2.coordinator;

import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.coordinator.JobStateProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader-owned job/task checkpoint store with monotonic progress event numbers.
 *
 * <p>Every mutation (job admitted, task assigned, task completed, job finished)
 * is assigned the next global progress number. Leaders push snapshots with
 * that number via {@code ReplicateJobState}; followers keep the snapshot with
 * the highest progress number per job, so a successor resumes unfinished jobs
 * without client help.
 */
public final class ReplicatedJobStore {
    public enum TaskState { PENDING, RUNNING, COMPLETED, FAILED }
    public enum JobStatus { ACTIVE, COMPLETED, FAILED }

    public record TaskRecord(String taskId, WorkUnit unit, TaskState status,
                             int attempt, int assignedWorkerId,
                             Long resultValue, String lastFailure,
                             long progressIndex, String exactResultValue) {
        public TaskRecord(String taskId, WorkUnit unit, TaskState status,
                          int attempt, int assignedWorkerId, Long resultValue,
                          String lastFailure, long progressIndex) {
            this(taskId, unit, status, attempt, assignedWorkerId, resultValue,
                    lastFailure, progressIndex, "");
        }
    }

    private final AtomicLong progressSeq = new AtomicLong(0);
    private final ConcurrentHashMap<String, JobRecord> jobs = new ConcurrentHashMap<>();

    public long progress() {
        return progressSeq.get();
    }

    /** Advances the watermark to at least the given value (follower path). */
    public void observeProgress(long progressIndex) {
        progressSeq.accumulateAndGet(progressIndex, Math::max);
    }

    public long nextProgress() {
        return progressSeq.incrementAndGet();
    }

    /** Leader path: admit a job with deterministic PENDING task checkpoints. */
    public JobRecord createJob(JobRequest request, List<WorkUnit> units, long term) {
        long progress = nextProgress();
        JobRecord record = new JobRecord(request, term, progress);
        for (WorkUnit unit : units) {
            record.tasks.put(unit.partitionId(), new TaskRecord(
                    taskId(request.jobId(), unit.partitionId()), unit,
                    TaskState.PENDING, 0, -1, null, null, progress));
        }
        jobs.put(request.jobId(), record);
        return record.snapshot();
    }

    /** Leader path for jobs with no partitions (empty PRIMECOUNT). */
    public JobRecord createEmptyJob(JobRequest request, long term, long result) {
        long progress = nextProgress();
        JobRecord record = new JobRecord(request, term, progress);
        record.status = JobStatus.COMPLETED;
        record.resultValue = result;
        record.progressIndex = nextProgress();
        jobs.put(request.jobId(), record);
        return record.snapshot();
    }

    public JobRecord get(String jobId) {
        JobRecord record = jobs.get(jobId);
        return record == null ? null : record.snapshot();
    }

    /** Live reference for the leader dispatch loop (mutate under job lock). */
    JobRecord live(String jobId) {
        return jobs.get(jobId);
    }

    public List<JobRecord> snapshots() {
        List<JobRecord> out = new ArrayList<>();
        for (JobRecord record : jobs.values()) out.add(record.snapshot());
        out.sort(Comparator.comparing(JobRecord::jobId));
        return out;
    }

    public List<JobRecord> activeSnapshots() {
        List<JobRecord> out = new ArrayList<>();
        for (JobRecord record : jobs.values()) {
            if (record.status == JobStatus.ACTIVE) out.add(record.snapshot());
        }
        out.sort(Comparator.comparing(JobRecord::jobId));
        return out;
    }

    /**
     * Follower path: install a replicated snapshot when its progress number is
     * newer than the locally stored one. Returns true when applied.
     */
    public boolean applyReplicated(JobStateProto proto, long progressIndex) {
        JobRecord incoming = CoordinatorJobMapper.toJobRecord(proto);
        observeProgress(progressIndex);
        return jobs.compute(incoming.jobId(), (jobId, current) -> {
            if (current == null || progressIndex > current.progressIndex) {
                JobRecord stored = new JobRecord(incoming.request(), incoming.term(),
                        Math.max(progressIndex, incoming.progressIndex()));
                stored.status = incoming.status();
                stored.resultValue = incoming.resultValue();
                stored.exactResultValue = incoming.exactResultValue();
                stored.error = incoming.error();
                stored.progressIndex = Math.max(progressIndex, incoming.progressIndex());
                for (TaskRecord task : incoming.tasks()) {
                    stored.tasks.put(task.unit().partitionId(), task);
                }
                return stored;
            }
            return current;
        }) != null;
    }

    static String taskId(String jobId, int partitionId) {
        return jobId + ":task:" + partitionId;
    }

    /** Mutable leader-side record; snapshots are immutable copies. */
    public static final class JobRecord {
        private final JobRequest request;
        final ConcurrentHashMap<Integer, TaskRecord> tasks = new ConcurrentHashMap<>();
        volatile JobStatus status = JobStatus.ACTIVE;
        volatile Long resultValue;
        volatile String exactResultValue = "";
        volatile String error;
        volatile long progressIndex;
        private final long term;

        JobRecord(JobRequest request, long term, long progressIndex) {
            this.request = request;
            this.term = term;
            this.progressIndex = progressIndex;
        }

        JobRecord(JobRequest request, List<TaskRecord> tasks, JobStatus status,
                  Long resultValue, String exactResultValue, String error,
                  long progressIndex, long term) {
            this.request = request;
            this.term = term;
            this.progressIndex = progressIndex;
            this.status = status;
            this.resultValue = resultValue;
            this.exactResultValue = exactResultValue;
            this.error = error;
            for (TaskRecord task : tasks) this.tasks.put(task.unit().partitionId(), task);
        }

        public String jobId() { return request.jobId(); }
        public JobRequest request() { return request; }
        public JobStatus status() { return status; }
        public Long resultValue() { return resultValue; }
        public String exactResultValue() { return exactResultValue; }
        public String error() { return error; }
        public long progressIndex() { return progressIndex; }
        public long term() { return term; }

        public List<TaskRecord> tasks() {
            List<TaskRecord> out = new ArrayList<>(tasks.values());
            out.sort(Comparator.comparingInt(t -> t.unit().partitionId()));
            return out;
        }

        synchronized JobRecord snapshot() {
            return new JobRecord(request, tasks(), status, resultValue, exactResultValue, error,
                    progressIndex, term);
        }
    }
}
