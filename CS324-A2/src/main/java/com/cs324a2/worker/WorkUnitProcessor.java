package com.cs324a2.worker;

import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.compute.ComputeEngine;

/** Executes a validated work unit using the shared computation implementation. */
public final class WorkUnitProcessor implements WorkUnitExecutor {
    @Override
    public WorkResult execute(WorkUnit unit) {
        if (unit == null) throw new IllegalArgumentException("work unit must not be null");
        long value = switch (unit.operation()) {
            case MAX -> ComputeEngine.max(unit.numbers());
            case PRIMESUM -> ComputeEngine.primeSum(unit.start(), unit.end());
            case PRIMECOUNT -> ComputeEngine.primeCount(unit.numbers());
        };
        return new WorkResult(unit.jobId(), unit.partitionId(), unit.operation(), value);
    }
}
