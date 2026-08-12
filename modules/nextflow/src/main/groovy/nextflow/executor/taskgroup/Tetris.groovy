package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import nextflow.executor.analyzer.TaskNode
import nextflow.util.Duration
import nextflow.util.MemoryUnit

/**
 * A {@link GroupingPolicy} based on the Tetris scheduling heuristic (Grandl et al., 2014).
 *
 * All (task, group) pairs are scored simultaneously using the dot product of the
 * task's raw resource vector and the group's remaining-capacity vector. Pairs are then
 * assigned greedily in score-descending order so that the best-fitting tasks are placed
 * first. Only dimensions with a configured node ceiling contribute to the score.
 *
 * @author Beatriz Cepa
 */
@CompileStatic
class Tetris implements GroupingPolicy {

    private static class ScoredPair {
        final TaskNode task
        final TaskGroup group
        final double score
        ScoredPair(TaskNode t, TaskGroup g, double s) { task = t; group = g; score = s }
    }

    @Override
    Map<TaskNode, TaskGroup> placeAll(List<TaskNode> tasks, List<TaskGroup> openGroups,
                                      int nodeMaxCpus, MemoryUnit nodeMaxMem, Duration nodeMaxTime) {
        // Step 1: Score every valid (task, group) pair.
        final List<ScoredPair> pairs = new ArrayList<>()
        for (TaskNode task : tasks) {
            for (TaskGroup g : openGroups) {
                if (fitsCapacity(task, g, nodeMaxCpus, nodeMaxMem, 0, 0L))
                    pairs.add(new ScoredPair(task, g, dotProduct(task, g, nodeMaxCpus, nodeMaxMem, nodeMaxTime)))
            }
        }

        // Step 2: Sort descending — highest-alignment pairs are assigned first.
        pairs.sort { ScoredPair a, ScoredPair b -> Double.compare(b.score, a.score) }

        // Step 3: Greedy assignment. Shadow maps track resources tentatively reserved
        //         within this call so that later pairs see up-to-date remaining capacity.
        final Map<TaskNode, TaskGroup> assignments = new LinkedHashMap<>()
        final Map<TaskGroup, Integer> shadowCpus = new IdentityHashMap<>()
        final Map<TaskGroup, Long>    shadowMem  = new IdentityHashMap<>()

        for (ScoredPair pair : pairs) {
            if (assignments.containsKey(pair.task)) continue
            final int  sC = shadowCpus.get(pair.group) ?: 0
            final long sM = shadowMem.get(pair.group)  ?: 0L
            if (!fitsCapacity(pair.task, pair.group, nodeMaxCpus, nodeMaxMem, sC, sM)) continue

            assignments.put(pair.task, pair.group)
            shadowCpus.put(pair.group, sC + pair.task.getCpus())
            if (pair.task.getMemory())
                shadowMem.put(pair.group, sM + pair.task.getMemory().toBytes())
        }

        // Step 4: Tasks with no valid pair -> null; caller opens a new group.
        for (TaskNode task : tasks)
            if (!assignments.containsKey(task))
                assignments.put(task, null)

        return assignments
    }

    /**
     * Dot product of the task's resource vector against the group's remaining-capacity
     * vector. Raw (un-normalised) values are used; only dimensions with a configured
     * node ceiling contribute to the score.
     *
     * For CPUs and memory, tasks run concurrently so the group accumulates the sum:
     *   remaining = nodeMax - groupTotal
     * For walltime, tasks also run concurrently so the group walltime is the maximum
     * of its members:
     *   remaining = nodeMaxTime - groupMaxTime
     */
    private static double dotProduct(TaskNode task, TaskGroup group,
                                     int nodeMaxCpus, MemoryUnit nodeMaxMem, Duration nodeMaxTime) {
        double score = 0.0d

        if (nodeMaxCpus > 0) {
            final long remainingCpus = nodeMaxCpus - group.getTotalCpus()
            score += (double) task.getCpus() * remainingCpus
        }

        if (nodeMaxMem != null) {
            final long usedMem = group.getTotalMemory() ? group.getTotalMemory().toBytes() : 0L
            final long taskMem = task.getMemory() ? task.getMemory().toBytes() : 0L
            score += (double) taskMem * (nodeMaxMem.toBytes() - usedMem)
        }

        if (nodeMaxTime != null) {
            final long usedMs = group.getMaxTime() ? group.getMaxTime().toMillis() : 0L
            final long taskMs = task.getTime() ? task.getTime().toMillis() : 0L
            score += (double) taskMs * (nodeMaxTime.toMillis() - usedMs)
        }

        return score
    }

    private static boolean fitsCapacity(TaskNode task, TaskGroup group,
                                        int nodeMaxCpus, MemoryUnit nodeMaxMem,
                                        int shadowCpus, long shadowMem) {
        if (group.getQueue() != null && task.getQueue() != group.getQueue()) return false

        if (nodeMaxCpus > 0 && group.getTotalCpus() + shadowCpus + task.getCpus() > nodeMaxCpus) return false

        if (nodeMaxMem != null) {
            final long baseMem = group.getTotalMemory() ? group.getTotalMemory().toBytes() : 0L
            final long taskMem = task.getMemory() ? task.getMemory().toBytes() : 0L
            if (baseMem + shadowMem + taskMem > nodeMaxMem.toBytes()) return false
        }
        return true
    }
}
