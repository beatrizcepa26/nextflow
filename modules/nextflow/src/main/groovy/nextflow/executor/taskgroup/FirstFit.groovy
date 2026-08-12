package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import nextflow.executor.analyzer.TaskNode
import nextflow.util.Duration
import nextflow.util.MemoryUnit

@CompileStatic
class FirstFit implements GroupingPolicy {

    @Override
    Map<TaskNode, TaskGroup> placeAll(List<TaskNode> tasks, List<TaskGroup> openGroups,
                                      int nodeMaxCpus, MemoryUnit nodeMaxMem, Duration nodeMaxTime) {
        final Map<TaskNode, TaskGroup> result = new LinkedHashMap<>()
        // Shadow maps track resources tentatively reserved within this call so that
        // multiple tasks assigned to the same group don't exceed its capacity.
        final Map<TaskGroup, Integer> shadowCpus = new IdentityHashMap<>()
        final Map<TaskGroup, Long>    shadowMem  = new IdentityHashMap<>()

        for (TaskNode task : tasks) {
            TaskGroup placed = null
            for (TaskGroup g : openGroups) {
                if (canFit(task, g, nodeMaxCpus, nodeMaxMem, shadowCpus, shadowMem)) {
                    placed = g
                    break
                }
            }
            result.put(task, placed)
            if (placed != null) {
                shadowCpus.put(placed, (shadowCpus.get(placed) ?: 0) + task.getCpus())
                if (task.getMemory())
                    shadowMem.put(placed, (shadowMem.get(placed) ?: 0L) + task.getMemory().toBytes())
            }
        }
        return result
    }

    static boolean canFit(TaskNode task, TaskGroup group, int nodeMaxCpus, MemoryUnit nodeMaxMem,
                           Map<TaskGroup, Integer> shadowCpus = [:], Map<TaskGroup, Long> shadowMem = [:]) {
        if (group.getQueue() != null && task.getQueue() != group.getQueue()) return false

        final int effectiveCpus = group.getTotalCpus() + (shadowCpus.get(group) ?: 0)
        if (nodeMaxCpus > 0 && effectiveCpus + task.getCpus() > nodeMaxCpus) return false

        if (nodeMaxMem != null) {
            final long baseMem = group.getTotalMemory() ? group.getTotalMemory().toBytes() : 0L
            final long shadow  = shadowMem.get(group) ?: 0L
            final long taskMem = task.getMemory() ? task.getMemory().toBytes() : 0L
            if (baseMem + shadow + taskMem > nodeMaxMem.toBytes()) return false
        }
        return true
    }
}