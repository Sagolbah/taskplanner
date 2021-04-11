package ru.ifmo.jbinternship.taskplanner;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TaskExecutor {
    /**
     * Executes provided tasks concurrently with all available processors.
     *
     * @param tasks collection of tasks to execute
     * @throws TaskExecutionException if dependency graph contains cycles or provided task failed with exception
     */
    public void execute(Collection<Task> tasks) throws TaskExecutionException {
        execute(tasks, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Executes provided tasks with certain number of threads.
     *
     * @param tasks        collection of tasks to execute
     * @param threadsCount number of working threads
     * @throws TaskExecutionException if dependency graph contains cycles or provided task failed with exception
     */
    public void execute(Collection<Task> tasks, int threadsCount) throws TaskExecutionException {
        // Initialize graph and pool
        Set<Task> uniqueTasks = new HashSet<>(tasks);
        if (uniqueTasks.size() != tasks.size()) {
            throw new TaskExecutionException("Duplicate tasks are not allowed");
        }
        Map<Task, List<Task>> graph = buildGraph(uniqueTasks);
        validateGraph(graph);
        final ExecutorService pool = Executors.newFixedThreadPool(threadsCount);
        List<Task> start = tasks.stream().filter(x -> x.dependencies().isEmpty()).collect(Collectors.toList());
        try {
            Map<Task, Integer> required = new ConcurrentHashMap<>();
            for (final Task t : uniqueTasks) {
                required.put(t, t.dependencies().size());
            }
            final Phaser phaser = new Phaser(1);
            Set<Future<?>> submittedTasks = ConcurrentHashMap.newKeySet();
            start.forEach(x -> submitTask(x, graph, submittedTasks, required, phaser, pool));
            phaser.arriveAndAwaitAdvance();
            for (final Future<?> checker : submittedTasks) {
                // Invariant: All tasks are completed. This is for exception check only.
                checker.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new TaskExecutionException("Parallel processing failed", e);
        } finally {
            pool.shutdownNow();
        }
    }

    private void submitTask(Task task, Map<Task, List<Task>> graph, Set<Future<?>> submittedTasks,
                            Map<Task, Integer> required, Phaser phaser, ExecutorService pool) {
        phaser.register();
        Callable<Void> newTask = () -> {
            try {
                bfs(task, graph, submittedTasks, required, phaser, pool);
            } finally {
                phaser.arrive();
            }
            return null;
        };
        submittedTasks.add(pool.submit(newTask));
    }

    private void bfs(Task current, Map<Task, List<Task>> graph, Set<Future<?>> submittedTasks,
                     Map<Task, Integer> required, Phaser phaser, ExecutorService pool) {
        List<Task> edges = graph.get(current);
        current.execute();
        for (final Task edge : edges) {
            Integer newValue = required.computeIfPresent(edge, (k, v) -> v - 1);
            assert newValue != null;
            if (newValue == 0) {
                submitTask(edge, graph, submittedTasks, required, phaser, pool);
            }
        }
    }

    // Constructs dependency graph
    private Map<Task, List<Task>> buildGraph(final Set<Task> tasks) throws TaskExecutionException {
        Map<Task, List<Task>> result = new HashMap<>();
        for (final Task t : tasks) {
            result.put(t, new ArrayList<>());
        }
        for (final Task t : tasks) {
            for (final Task dep : t.dependencies()) {
                List<Task> cur = result.get(dep);
                if (cur == null) {
                    throw new TaskExecutionException("One of tasks requires dependency, which is not present");
                }
                result.get(dep).add(t);
            }
        }
        return result;
    }

    // Checks if given dependency graph contains unknown dependencies, cycles or self-loops.
    private void validateGraph(final Map<Task, List<Task>> graph) throws TaskExecutionException {
        Map<Task, Integer> visited = new HashMap<>();
        for (final Task t : graph.keySet()) {
            if (visited.getOrDefault(t, 0) == 0) {
                validationDFS(graph, t, visited);
            }
        }
    }

    private void validationDFS(Map<Task, List<Task>> graph, Task curVertex, Map<Task, Integer> visited) throws TaskExecutionException {
        visited.put(curVertex, 1);
        for (final Task edge : graph.get(curVertex)) {
            int visitedState = visited.getOrDefault(edge, 0);
            if (visitedState == 1) {
                throw new TaskExecutionException("Dependency graph is not acyclic");
            } else if (visitedState == 0) {
                validationDFS(graph, edge, visited);
            }
        }
        visited.put(curVertex, 2);
    }

}
