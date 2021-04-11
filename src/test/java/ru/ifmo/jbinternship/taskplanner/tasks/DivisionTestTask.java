package ru.ifmo.jbinternship.taskplanner.tasks;

import ru.ifmo.jbinternship.taskplanner.Task;

import java.util.Iterator;
import java.util.List;

import static ru.ifmo.jbinternship.taskplanner.TaskExecutorTest.resultCollector;


public class DivisionTestTask extends TestTask {
    public DivisionTestTask(List<Task> dependencies) {
        super(dependencies);
    }

    @Override
    protected void executionImpl() {
        if (dependencies().size() != 2) {
            throw new IllegalArgumentException("DivisionTask must have exactly 2 arguments");
        }
        Iterator<Task> iter = dependencies().iterator();
        value = resultCollector.get(iter.next()) / resultCollector.get(iter.next());
    }
}
