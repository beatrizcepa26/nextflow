
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
 * Uses first-fit placement inside each level.
 *
 * @author Beatriz Cepa
 */
@Slf4j
@CompileStatic
class TaskGroupBuilder {

    private final SlurmTaskGroupAnalyzer analyzer
    private static int groupId = 0

    TaskGroupBuilder(SlurmTaskGroupAnalyzer analyzer) {
        this.analyzer = analyzer
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

        for( TaskNode task : tasks ) {
            TaskGroup placed = null
            for( TaskGroup group : groups ) {
                if( canFit(task, group) ) {
                    group.addTask(task)
                    placed = group
                    break
                }
            }

            if( placed == null ) {
                if( !fitsNodeCapacity(task) ) {
                    throw new IllegalStateException("[SLURM TASK GROUPING] Task '${task.getName()}' exceeds node capacity: cpus=${task.getCpus()}, memory=${task.getMemory()}, time=${task.getTime()}")
                }

                final TaskGroup group = new TaskGroup(level, ++groupId)
                group.addTask(task)
                groups.add(group)
            }
        }

        log.debug "[SLURM TASK GROUPING] Level ${level} packed into ${groups.size()} group(s)"
        return groups
    }

    private boolean fitsNodeCapacity(TaskNode task) {
        final int maxCpus = analyzer.getNodeMaxCpus()
        if( maxCpus > 0 && task.getCpus() > maxCpus )
            return false

        final MemoryUnit maxMemory = analyzer.getNodeMaxMemory()
        if( maxMemory != null && task.getMemory() != null && task.getMemory().compareTo(maxMemory) > 0 )
            return false

        final Duration maxTime = analyzer.getNodeMaxTime()
        if( maxTime != null && task.getTime() != null && task.getTime().compareTo(maxTime) > 0 )
            return false

        return true
    }

    private boolean canFit(TaskNode task, TaskGroup group) {
        if( group.getQueue() != null && task.getQueue() != group.getQueue() )
            return false

        final int maxCpus = analyzer.getNodeMaxCpus()
        if( maxCpus > 0 && (group.getTotalCpus() + task.getCpus()) > maxCpus )
            return false

        final MemoryUnit maxMemory = analyzer.getNodeMaxMemory()
        if( maxMemory != null ) {
            final MemoryUnit current = group.getTotalMemory()
            final MemoryUnit incoming = task.getMemory()
            final MemoryUnit candidate = incoming != null ? current.plus(incoming) : current
            if( candidate.compareTo(maxMemory) > 0 )
                return false
        }

        final Duration maxTime = analyzer.getNodeMaxTime()
        if( maxTime != null && task.getTime() != null ) {
            final Duration candidate = task.getTime().compareTo(group.getMaxTime()) > 0 ? task.getTime() : group.getMaxTime()
            if( candidate.compareTo(maxTime) > 0 )
                return false
        }

        return true
    }
}
