package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.List;

public class ConstTestTask extends TestTask {

    public ConstTestTask(List<Task> dependencies, int value) {
        super(dependencies);
        this.value = value;
    }

    @Override
    protected void executionImpl() {
        // Return constant, no operations.
    }

}
