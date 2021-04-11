package ru.ifmo.jbinternship.taskplanner;

import java.util.Collection;

/**
 * Interface for tasks to be executed.
 *
 * @author Daniil Boger
 */
public interface Task {
    /**
     * Executes the task
     */
    void execute();

    /**
     * Returns collection of task dependencies.
     *
     * @return collection of task dependencies
     */
    Collection<Task> dependencies();
}
