package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.List;

import static ru.ifmo.jbinternship.taskplanner.TaskExecutorTest.resultCollector;

public class XorTestTask extends TestTask {
    public XorTestTask(List<Task> dependencies) {
        super(dependencies);
    }

    @Override
    protected void executionImpl() {
        int ans = 0;
        for (Task t : dependencies()) {
            ans ^= resultCollector.get(t);
        }
        value = ans;
    }
}
