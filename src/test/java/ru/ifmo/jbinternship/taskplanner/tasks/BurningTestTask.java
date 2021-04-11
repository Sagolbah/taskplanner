package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.List;

import static ru.ifmo.jbinternship.taskplanner.TaskExecutorTest.resultCollector;

public class BurningTestTask extends TestTask {
    private static final int BURN_DURATION = 2_500_000;

    public BurningTestTask(List<Task> dependencies) {
        super(dependencies);
    }

    @Override
    protected void executionImpl() {
        int ans = 0;
        for (Task t : dependencies()) {
            ans += resultCollector.get(t);
        }
        for (int i = 0; i < BURN_DURATION; i++) {
            ans += i;
        }
        for (int i = 0; i < BURN_DURATION; i++) {
            ans -= i;
        }
        value = ans;
    }
}
