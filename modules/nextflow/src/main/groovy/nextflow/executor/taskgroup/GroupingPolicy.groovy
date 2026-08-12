package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import nextflow.executor.analyzer.TaskNode
import nextflow.util.Duration
import nextflow.util.MemoryUnit

@CompileStatic
interface GroupingPolicy {
    /**
     * Place all {@code tasks} into open groups.
     *
     * Returns a map of task → group assignment. Tasks mapped to {@code null} have no
     * fitting open group; the caller is responsible for opening a new group for them.
     *
     * @param tasks        tasks to place (all at the same level)
     * @param openGroups   currently open, not-yet-submitted groups
     * @param nodeMaxCpus  hard CPU ceiling per node (0 = unlimited)
     * @param nodeMaxMem   hard memory ceiling per node (null = unlimited)
     * @param nodeMaxTime  hard walltime ceiling per node (null = unlimited)
     */
    Map<TaskNode, TaskGroup> placeAll(List<TaskNode> tasks, List<TaskGroup> openGroups,
                                      int nodeMaxCpus, MemoryUnit nodeMaxMem, Duration nodeMaxTime)
}