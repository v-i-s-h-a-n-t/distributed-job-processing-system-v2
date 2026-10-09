package com.cs324a2.worker;

import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;

@FunctionalInterface
public interface WorkUnitExecutor {
    WorkResult execute(WorkUnit unit);
}
