package ru.ifmo.jbinternship.taskplanner;

import static org.junit.Assert.assertEquals;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import ru.ifmo.jbinternship.taskplanner.tasks.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for {@link TaskExecutor} class
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaskExecutorTest {
    private final TaskExecutor executor = new TaskExecutor();
    private final Random rng = new Random();
    private final List<Task> tasks = new ArrayList<>();
    public static Map<Task, Integer> resultCollector = new ConcurrentHashMap<>();
    private static List<Task> warmup;
    private static final int TREE_DEPTH = 12;

    @BeforeClass
    public static void prepareWarmup() {
        warmup = new ArrayList<>(Collections.nCopies(1 << TREE_DEPTH, null));
        // Binary tree
        for (int i = 0; i < (1 << (TREE_DEPTH - 1)); i++) {
            warmup.set((1 << (TREE_DEPTH - 1)) + i, new ConstTestTask(List.of(), 1));
        }
        for (int i = (1 << (TREE_DEPTH - 1)) - 1; i >= 1; i--) {
            warmup.set(i, new BurningTestTask(List.of(warmup.get(2 * i), warmup.get(2 * i + 1))));
        }
        warmup.set(0, new ConstTestTask(List.of(), 0));
    }

    @Before
    public void init() {
        tasks.clear();
        resultCollector.clear();
    }

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final String ACYCLIC_ERROR = "Dependency graph is not acyclic";
    private final String PROCESSING_ERROR = "Parallel processing failed";

    private void testFailure(final String expected) throws TaskExecutionException {
        expectedException.expect(TaskExecutionException.class);
        expectedException.expectMessage(expected);
        executor.execute(tasks, 4);
    }

    @Test
    public void test01_selfLoop() throws TaskExecutionException {
        Task t = new ConstTestTask(new ArrayList<>(), 0);
        t.dependencies().add(t);
        tasks.add(t);
        testFailure(ACYCLIC_ERROR);
    }

    @Test
    public void test02_unknownTask() throws TaskExecutionException {
        Task unknown = new ConstTestTask(new ArrayList<>(), 0);
        Task t = new ConstTestTask(List.of(unknown), 0);
        tasks.add(t);
        String UNKNOWN_DEPENDENCY_ERROR = "One of tasks requires dependency, which is not present";
        testFailure(UNKNOWN_DEPENDENCY_ERROR);
    }

    @Test
    public void test03_triangle() throws TaskExecutionException {
        Task t1 = new ConstTestTask(new ArrayList<>(), 0);
        Task t2 = new ConstTestTask(List.of(t1), 0);
        Task t3 = new ConstTestTask(List.of(t2), 0);
        t1.dependencies().add(t3);
        tasks.add(t1);
        tasks.add(t2);
        tasks.add(t3);
        testFailure(ACYCLIC_ERROR);
    }

    @Test
    public void test04_undirectedEdge() throws TaskExecutionException {
        Task t1 = new ConstTestTask(new ArrayList<>(), 0);
        Task t2 = new ConstTestTask(List.of(t1), 0);
        t1.dependencies().add(t2);
        tasks.add(t1);
        tasks.add(t2);
        testFailure(ACYCLIC_ERROR);
    }

    @Test
    public void test05_const() throws TaskExecutionException {
        Task t = new ConstTestTask(List.of(), 5);
        tasks.add(t);
        executor.execute(tasks, 4);
        assertEquals(Map.of(t, 5), resultCollector);
    }

    @Test
    public void test06_xor() throws TaskExecutionException {
        Task t1 = new ConstTestTask(List.of(), 2);
        Task t2 = new ConstTestTask(List.of(), 7);
        Task t3 = new XorTestTask(List.of(t1, t2));
        tasks.add(t1);
        tasks.add(t2);
        tasks.add(t3);
        executor.execute(tasks, 4);
        assertEquals(Map.of(t1, 2, t2, 7, t3, 5), resultCollector);
    }

    @Test
    public void test07_simpleBurn() throws TaskExecutionException {
        Task t1 = new ConstTestTask(List.of(), 2);
        Task t2 = new ConstTestTask(List.of(), 7);
        Task t3 = new ConstTestTask(List.of(), 6);
        Task burn = new BurningTestTask(List.of(t1, t2));
        Task xor = new XorTestTask(List.of(burn, t3));
        tasks.add(t1);
        tasks.add(t2);
        tasks.add(t3);
        tasks.add(burn);
        tasks.add(xor);
        executor.execute(tasks, 4);
        assertEquals(Map.of(t1, 2, t2, 7, t3, 6, burn, 9, xor, 15), resultCollector);
    }

    // Exceptions
    @Test
    public void test08_exception() throws TaskExecutionException {
        Task t1 = new ConstTestTask(List.of(), 2);
        Task t2 = new ConstTestTask(List.of(), 7);
        Task fail = new DivisionTestTask(List.of(t1));
        Task burn = new BurningTestTask(List.of(t1, fail));
        tasks.addAll(List.of(t1, t2, burn, fail));
        testFailure(PROCESSING_ERROR);
    }

    @Test
    public void test09_exceptionDivisionByZero() throws TaskExecutionException {
        Task t1 = new ConstTestTask(List.of(), 2);
        Task t2 = new ConstTestTask(List.of(), 0);
        Task fail = new DivisionTestTask(List.of(t1, t2));
        Task burn = new BurningTestTask(List.of(t1, fail));
        tasks.addAll(List.of(t1, t2, burn, fail));
        testFailure(PROCESSING_ERROR);
    }

    @Test
    public void test10_advancedException() throws TaskExecutionException {
        tasks.addAll(warmup);
        tasks.set(0, new DivisionTestTask(List.of(warmup.get(1), warmup.get(2), warmup.get(3))));
        testFailure(PROCESSING_ERROR);
    }


    // Burning section

    @Test
    public void test11_burnBinaryTree() throws TaskExecutionException {
        tasks.addAll(warmup);
        Map<Task, Integer> expected = new HashMap<>();
        for (int i = 0; i < TREE_DEPTH; i++) {
            for (int j = 0; j < (1 << i); j++) {
                expected.put(warmup.get((1 << i) + j), 1 << (TREE_DEPTH - i - 1));
            }
        }
        expected.put(warmup.get(0), 0);
        burningTest(expected, 8, "Binary tree");
    }

    @Test
    public void test12_burnSeparated() throws TaskExecutionException {
        final int burnLowerBound = 1_000_000;
        Map<Task, Integer> expected = new HashMap<>();
        for (int i = 0; i < 20000; i++) {
            int correct = rng.nextInt(10000);
            Task t = new ConstBurningTestTask(List.of(), correct, burnLowerBound + rng.nextInt(9_000_000));
            tasks.add(t);
            expected.put(t, correct);
        }
        burningTest(expected, 4, "Separated tasks (no dependencies)");
    }

    @Test
    public void test13_smallGraphsStress() throws TaskExecutionException {
        System.err.println("Running stress test: Small graphs");
        stress(100, 50);
    }

    @Test
    public void test14_mediumGraphsStress() throws TaskExecutionException {
        System.err.println("Running stress test: Medium graphs");
        stress(20, 300);
    }

    @Test
    public void test15_largeGraphsStress() throws TaskExecutionException {
        System.err.println("Running stress test: Large graphs");
        stress(5, 2500);
    }

    @Test
    public void test16_burnMediumSample() throws TaskExecutionException {
        DAGTestPair test = generateDAG(1000);
        tasks.addAll(test.getTest());
        burningTest(test.getAnswer(), 4, "Medium random DAG sample");
    }

    @Test
    public void test17_burnLargeSample() throws TaskExecutionException {
        DAGTestPair test = generateDAG(5000);
        tasks.addAll(test.getTest());
        burningTest(test.getAnswer(), 6, "Large random DAG sample");
    }


    private void stress(int iterations, int graphSize) throws TaskExecutionException {
        for (int i = 0; i < iterations; i++) {
            System.err.println("Progress: " + (i + 1) + "/" + iterations);
            DAGTestPair test = generateDAG(graphSize);
            tasks.addAll(test.getTest());
            executor.execute(tasks, 8);
            assertEquals(test.getAnswer(), resultCollector);
            resultCollector.clear();
            tasks.clear();
        }
        System.err.println("OK");
    }


    private void burningTest(Map<Task, Integer> expected, int threads, String message) throws TaskExecutionException {
        System.err.println("Running burning test: " + message);
        System.err.println("== Warm up ==");
        for (int i = 0; i < 5; i++) {
            executor.execute(warmup);
        }
        resultCollector.clear();
        System.err.println("== Measurement ==");
        long start = System.nanoTime();
        executor.execute(tasks, threads);
        long parallel = System.nanoTime() - start;
        start = System.nanoTime();
        executor.execute(tasks, 1);
        long sequential = System.nanoTime() - start;
        System.err.format("=== Performance ratio %.3f for %d threads (%.3f s. parallel, %.3f s. sequential)%n",
                (double) sequential / parallel, threads, parallel / 1e9, sequential / 1e9);
        assertEquals(expected, resultCollector);
    }

    private DAGTestPair generateDAG(int size) {
        List<List<Integer>> graph = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            graph.add(new ArrayList<>());
        }
        final double edgeProbability = 0.35;
        final double burningProbability = 0.8;
        final int constBound = 10;
        // Create edges
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                if (Math.random() <= edgeProbability) {
                    graph.get(i).add(j);
                }
            }
        }
        // Distribute tasks
        // Representation: (-1) - burning, non-negative - const
        List<Integer> actions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (Math.random() <= burningProbability) {
                actions.add(-1);
            } else {
                actions.add(rng.nextInt(constBound));
            }
        }
        List<Task> generatedTest = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (actions.get(i) == -1) {
                generatedTest.add(new BurningTestTask(new ArrayList<>()));
            } else {
                generatedTest.add(new ConstTestTask(new ArrayList<>(), actions.get(i)));
            }
        }
        for (int i = 0; i < size; i++) {
            Task cur = generatedTest.get(i);
            for (final Integer dependency : graph.get(i)) {
                cur.dependencies().add(generatedTest.get(dependency));
            }
        }
        int[] answers = new int[size];
        boolean[] visited = new boolean[size];
        List<Integer> topologicalSort = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (!visited[i]) {
                topologicalSort(i, visited, graph, topologicalSort);
            }
        }
        Collections.reverse(topologicalSort);
        //System.err.println("Topological sort: " + topologicalSort.toString());
        for (int i = 0; i < size; i++) {
            visited[i] = false;
        }
        for (final Integer i : topologicalSort) {
            if (!visited[i]) {
                prepareAnswer(i, visited, answers, actions, graph);
            }
        }
        Map<Task, Integer> correct = new HashMap<>();
        for (int i = 0; i < size; i++) {
            correct.put(generatedTest.get(i), answers[i]);
        }
        return new DAGTestPair(generatedTest, correct);
    }

    private void topologicalSort(int current, boolean[] visited, List<List<Integer>> graph, List<Integer> result) {
        visited[current] = true;
        for (final Integer edge : graph.get(current)) {
            if (!visited[edge]) {
                topologicalSort(edge, visited, graph, result);
            }
        }
        result.add(current);
    }

    private int prepareAnswer(int current, boolean[] visited, int[] answers, List<Integer> actions, List<List<Integer>> graph) {
        visited[current] = true;
        int accumulator = 0;
        for (final Integer edge : graph.get(current)) {
            if (!visited[edge]) {
                accumulator += prepareAnswer(edge, visited, answers, actions, graph);
            } else {
                accumulator += answers[edge];
            }
        }
        if (actions.get(current) >= 0) {
            // Const
            answers[current] = actions.get(current);
        } else if (actions.get(current) == -1) {
            // Sum
            answers[current] = accumulator;
        }
        return answers[current];
    }


    private static class DAGTestPair {
        private final List<Task> test;
        private final Map<Task, Integer> answer;

        public DAGTestPair(List<Task> test, Map<Task, Integer> answer) {
            this.test = test;
            this.answer = answer;
        }

        public Map<Task, Integer> getAnswer() {
            return answer;
        }

        public List<Task> getTest() {
            return test;
        }
    }

}
