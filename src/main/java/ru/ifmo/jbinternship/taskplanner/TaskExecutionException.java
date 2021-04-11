package ru.ifmo.jbinternship.taskplanner;

public class TaskExecutionException extends Exception {
    public TaskExecutionException(final String message) {
        super(message);
    }

    public TaskExecutionException(final String message, final Throwable e) {
        super(message, e);
    }
}
