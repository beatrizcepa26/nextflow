
package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.analyzer.DependencyGraph
import nextflow.executor.analyzer.SlurmTaskGroupAnalyzer
import nextflow.executor.analyzer.TaskNode
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import java.util.stream.Collectors

/**
 * Builds task groups per level from the analyzer dependency graph, while
 * respecting configured node resource limits.
 *
 * The placement strategy is supplied via a {@link GroupingPolicy}. When no policy
 * is provided the default is {@link FirstFit}.
 *
 * @author Beatriz Cepa
 */
@Slf4j
@CompileStatic
class TaskGroupBuilder {

    private final SlurmTaskGroupAnalyzer analyzer
    private final GroupingPolicy policy
    private int groupId = 0

    TaskGroupBuilder(SlurmTaskGroupAnalyzer analyzer) {
        this(analyzer, new FirstFit())
    }

    TaskGroupBuilder(SlurmTaskGroupAnalyzer analyzer, GroupingPolicy policy) {
        this.analyzer = analyzer
        this.policy   = policy
    }

    /**
     * Build task groups for each assigned level.
     *
     * @return map of level -> list of groups in that level.
     */
    Map<Integer, List<TaskGroup>> build() {
        final DependencyGraph dependencyGraph = analyzer.analyzeDependencyGraph()
        if( dependencyGraph == null )
            return Collections.<Integer, List<TaskGroup>> emptyMap()

        dependencyGraph.assignLevelsIteratively()
        final Map<Integer, List<Long>> byLevel = dependencyGraph.groupByLevels()

        final Map<Integer, List<TaskGroup>> result = new LinkedHashMap<>() // preserve level order
        final List<Integer> levels = byLevel.keySet().stream()
            .sorted()
            .collect(Collectors.toList())

        for( Integer level : levels ) {
            final List<TaskNode> tasks = new ArrayList<>()
            final List<Long> ids = byLevel.get(level)
            if( ids != null ) {
                for( Long id : ids ) {
                    final TaskNode node = dependencyGraph.getNodes().get(id)
                    if( node != null )
                        tasks.add(node)
                }
            }

            // Place larger tasks first to reduce fragmentation.
            tasks.sort { TaskNode a, TaskNode b ->
                int c = Integer.compare(b.getCpus(), a.getCpus())
                if( c != 0 ) return c
                final long am = a.getMemory() ? a.getMemory().toBytes() : 0L
                final long bm = b.getMemory() ? b.getMemory().toBytes() : 0L
                return Long.compare(bm, am)
            }

            result.put(level, buildGroupsForLevel(level, tasks))
        }

        return result
    }

    private List<TaskGroup> buildGroupsForLevel(int level, List<TaskNode> tasks) {
        final List<TaskGroup> groups = new ArrayList<>()
        final int maxCpus = analyzer.getNodeMaxCpus()
        final MemoryUnit maxMem = analyzer.getNodeMaxMemory()
        final Duration maxTime = analyzer.getNodeMaxTime()

        for( TaskNode task : tasks )
            if( !fitsNodeCapacity(task, maxCpus, maxMem) )
                throw new IllegalStateException("[SLURM TASK GROUPING] Task '${task.getName()}' exceeds node capacity: cpus=${task.getCpus()}, memory=${task.getMemory()}")

        // Iterative placement: on each round the policy scores all remaining tasks against
        // all open groups. Tasks the policy cannot fit (null) are retried after a new empty
        // group is added. Every task fits an empty group (fitsNodeCapacity guarantees this),
        // so at least one task is placed per new group → termination is guaranteed.
        List<TaskNode> remaining = new ArrayList<>(tasks)
        while( !remaining.isEmpty() ) {
            final Map<TaskNode, TaskGroup> assignments = policy.placeAll(remaining, groups, maxCpus, maxMem, maxTime)
            final List<TaskNode> unplaced = new ArrayList<>()

            for( TaskNode task : remaining ) {
                final TaskGroup placed = assignments.get(task)
                if( placed != null ) placed.addTask(task)
                else unplaced.add(task)
            }

            if( !unplaced.isEmpty() && unplaced.size() == remaining.size() ) {
                // No task was placed - no open group could accommodate any of them.
                // Open one new empty group so the next round can make progress.
                groups.add(new TaskGroup(level, ++groupId))
            }
            remaining = unplaced
        }

        log.debug "[SLURM TASK GROUPING] Level ${level} packed into ${groups.size()} group(s)"
        return groups
    }

    private static boolean fitsNodeCapacity(TaskNode task, int maxCpus, MemoryUnit maxMem) {
        if( maxCpus > 0 && task.getCpus() > maxCpus )
            return false
        if( maxMem != null && task.getMemory() != null && task.getMemory().compareTo(maxMem) > 0 )
            return false
        return true
    }
}
