package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static ru.ifmo.jbinternship.taskplanner.TaskExecutorTest.resultCollector;

public abstract class TestTask implements Task {
    private final AtomicBoolean noMultipleCallsGuard = new AtomicBoolean(false);
    private final List<Task> dependencies;
    protected int value = 0;

    public TestTask(final List<Task> dependencies) {
        this.dependencies = dependencies;
    }

    protected abstract void executionImpl();

    public Collection<Task> dependencies() {
        return dependencies;
    }

    public void execute() {
        if (noMultipleCallsGuard.compareAndSet(false, true)) {
            try {
                executionImpl();
                resultCollector.put(this, value);
            } finally {
                noMultipleCallsGuard.set(false);
            }
        } else {
            throw new AssertionError("One task must not execute multiple times at once");
        }
    }


}
