package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.List;

public class ConstBurningTestTask extends TestTask {
    private final int burnDuration;

    public ConstBurningTestTask(List<Task> dependencies, int value, int burnDuration) {
        super(dependencies);
        this.value = value;
        this.burnDuration = burnDuration;
    }

    @Override
    protected void executionImpl() {
        for (int i = 0; i < burnDuration; i++) value++;
        value -= burnDuration;
    }

}
