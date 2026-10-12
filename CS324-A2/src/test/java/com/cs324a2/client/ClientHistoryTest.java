package com.cs324a2.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.swing.table.DefaultTableModel;
import org.junit.jupiter.api.Test;

/**
 * Headless checks for history clearing: terminal rows go away with their
 * progress state and stream handles, RUNNING rows (and their live streams)
 * are untouched.
 */
class ClientHistoryTest {

    @Test
    void removesDoneAndFailedKeepsRunning() {
        DefaultTableModel model = model(
                row("a", "RUNNING"), row("b", "DONE"), row("c", "FAILED"), row("d", "RUNNING"));
        Map<String, JobProgressUpdate> progress = new HashMap<>();
        progress.put("b", update("b"));
        progress.put("d", update("d"));
        Map<String, GrpcClientService.ProgressHandle> handles = new HashMap<>();
        AtomicBoolean bCancelled = new AtomicBoolean();
        AtomicBoolean cCancelled = new AtomicBoolean();
        AtomicBoolean dCancelled = new AtomicBoolean();
        handles.put("b", () -> bCancelled.set(true));
        handles.put("c", () -> cCancelled.set(true));
        handles.put("d", () -> dCancelled.set(true));

        assertEquals(2, ClientApp.removeFinishedRows(model, progress, handles));

        assertEquals(2, model.getRowCount());
        assertEquals("a", model.getValueAt(0, 0));
        assertEquals("d", model.getValueAt(1, 0));
        assertEquals(0, model.getValueAt(0, ClientApp.COL_PROGRESS));
        assertEquals("RUNNING", model.getValueAt(1, ClientApp.COL_STATUS));

        assertEquals(java.util.Set.of("d"), progress.keySet());
        assertEquals(java.util.Set.of("d"), handles.keySet());
        assertTrue(bCancelled.get());
        assertTrue(cCancelled.get());
        assertFalse(dCancelled.get());
    }

    @Test
    void noFinishedRowsIsNoop() {
        DefaultTableModel model = model(row("a", "RUNNING"));
        Map<String, JobProgressUpdate> progress = new HashMap<>();
        Map<String, GrpcClientService.ProgressHandle> handles = new HashMap<>();
        AtomicBoolean cancelled = new AtomicBoolean();
        handles.put("a", () -> cancelled.set(true));

        assertEquals(0, ClientApp.removeFinishedRows(model, progress, handles));

        assertEquals(1, model.getRowCount());
        assertEquals(java.util.Set.of("a"), handles.keySet());
        assertFalse(cancelled.get());
    }

    @Test
    void completedJobIsNormalisedToFullProgress() {
        // A job can finish before the terminal stream update is delivered (the
        // submit returns first). The bar must not stay frozen at the last
        // mid-flight percentage.
        JobProgressUpdate midFlight = JobProgressUpdate.newBuilder()
                .setJobId("j").setTotalTasks(12).setCompletedTasks(8)
                .setRunningTasks(4).setPendingTasks(0)
                .setCompletionFraction(8.0 / 12.0)
                .setStatus(com.cs324a2.grpc.coordinator.JobStatus.JOB_ACTIVE)
                .build();
        JobProgressUpdate done = ClientApp.asCompleted(midFlight);
        assertEquals(12, done.getCompletedTasks());
        assertEquals(0, done.getRunningTasks());
        assertEquals(0, done.getPendingTasks());
        assertEquals(1.0, done.getCompletionFraction(), 1e-9);
        assertEquals(com.cs324a2.grpc.coordinator.JobStatus.JOB_COMPLETED, done.getStatus());
        // Real totals are preserved, not fabricated.
        assertEquals(12, done.getTotalTasks());
        assertEquals("j", done.getJobId());
    }

    private static DefaultTableModel model(Object[]... rows) {
        DefaultTableModel model = new DefaultTableModel(
                new Object[]{"Job", "Operation", "Input", "Status", "Progress", "Result"}, 0);
        for (Object[] row : rows) model.addRow(row);
        return model;
    }

    private static Object[] row(String jobId, String status) {
        return new Object[]{jobId, "MAX", "[1]", status, 0, ""};
    }

    private static JobProgressUpdate update(String jobId) {
        return JobProgressUpdate.newBuilder().setJobId(jobId)
                .setTotalTasks(3).setCompletedTasks(1).setCompletionFraction(1.0 / 3.0)
                .build();
    }
}
