/*
 * Copyright 2013-2026, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.DAG
import nextflow.executor.analyzer.SlurmTaskGroupAnalyzer
import nextflow.executor.analyzer.TaskNode
import nextflow.executor.taskgroup.FirstFit
import nextflow.executor.taskgroup.GroupingPolicy
import nextflow.executor.taskgroup.TaskGroup
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.MemoryUnit

import java.nio.file.Path

/**
 * Coordinates the dynamic, per-level buffering and batching of {@link GridTaskHandler}s
 * whose processes have been mapped to a level via static DAG analysis.
 *
 * Unlike the original design — which required exactly one handler per distinct process of
 * a single, statically pre-computed {@link TaskGroup} before submitting anything — this
 * coordinator forms runtime groups on the fly, per level:
 * <ul>
 *   <li>Each arriving handler is placed into an open group for its process's level using
 *       the configured {@link GroupingPolicy} (e.g. {@code FirstFit} or {@code Tetris}),
 *       weighed against the group's accumulated resources and node capacity.</li>
 *   <li>A group is submitted the moment it reaches node capacity, without waiting for
 *       slower sibling processes — this is what prevents fast downstream tasks from
 *       stalling behind long-running upstream ones.</li>
 *   <li>Any groups still open once every distinct process at that level has contributed
 *       one handler ("round complete") are flushed together, guaranteeing forward
 *       progress even when node capacity is never fully reached.</li>
 * </ul>
 *
 * A process that emits multiple {@link TaskRun}s only ever contributes one handler per
 * round; further handlers are queued until the current round completes, so a group's
 * accumulated resources are never double-counted for the same process.
 *
 * @author Beatriz Cepa
 */
@Slf4j
@CompileStatic
class TaskGroupCoordinator {

    /** The runtime group and the handlers that were placed into it, ready for submission. */
    static class GroupBatch {
        final TaskGroup group
        final List<GridTaskHandler> handlers
        GroupBatch(TaskGroup group, List<GridTaskHandler> handlers) {
            this.group = group
            this.handlers = handlers
        }
    }

    /** Per-level buffering state for the current round of runtime group formation. */
    private static class LevelState {
        final List<TaskGroup> openGroups = new ArrayList<>()
        final Map<TaskGroup, List<GridTaskHandler>> handlers = new IdentityHashMap<>()
        final Set<TaskProcessor> contributed = new HashSet<>()
        final Map<TaskProcessor, ArrayDeque<GridTaskHandler>> pending = new HashMap<>()
    }

    private final Session session
    private final GroupingPolicy policy

    private int nodeMaxCpus
    private MemoryUnit nodeMaxMemory
    private Duration nodeMaxTime

    /** Maps TaskProcessor → its statically-derived TaskNode (resource profile, level, queue). */
    private final Map<TaskProcessor, TaskNode> processorToNode = new HashMap<>()

    /** Total number of distinct processes assigned to each level. */
    private final Map<Integer, Integer> levelProcessCount = new HashMap<>()

    /** Buffering state per level. */
    private final Map<Integer, LevelState> levelStates = new HashMap<>()

    private int groupId = 0

    TaskGroupCoordinator(Session session) {
        this(session, new FirstFit())
    }

    TaskGroupCoordinator(Session session, GroupingPolicy policy) {
        this.session = session
        this.policy  = policy
        initNodeCapacity()
        buildLookups()
    }

    private void initNodeCapacity() {
        final analyzer = new SlurmTaskGroupAnalyzer(session)
        analyzer.readNodeCapacity()
        nodeMaxCpus   = analyzer.getNodeMaxCpus()
        nodeMaxMemory = analyzer.getNodeMaxMemory()
        nodeMaxTime   = analyzer.getNodeMaxTime()
    }

    private void buildLookups() {
        final Map<Long, TaskNode> nodeById = new HashMap<>()
        for (List<TaskGroup> groups : session.getTaskGroups().values())
            for (TaskGroup group : groups)
                for (TaskNode node : group.getTasks())
                    nodeById.put(node.getId(), node)

        if (session.dag == null) return

        for (DAG.Vertex vertex : session.dag.vertices) {
            if (vertex.type == DAG.Type.PROCESS && vertex.process != null) {
                final TaskNode node = nodeById.get(vertex.id)
                if (node != null) {
                    processorToNode.put(vertex.process, node)
                    levelProcessCount.put(node.getLevel(), (levelProcessCount.get(node.getLevel()) ?: 0) + 1)
                    log.debug "[SLURM TASK GROUPING] Process '${vertex.label}' mapped to level ${node.getLevel()}"
                }
            }
        }
        log.debug "[SLURM TASK GROUPING] Coordinator initialised: ${processorToNode.size()} process(es) across ${levelProcessCount.size()} level(s)"
    }

    boolean isGrouped(TaskRun task) {
        return processorToNode.containsKey(task.processor)
    }

    /**
     * Accept a handler for a grouped task and dynamically place it into an open runtime
     * group using the configured {@link GroupingPolicy}.
     *
     * @return the {@link GroupBatch}es that became ready for submission as a result of this
     *         offer — usually empty (still buffered) or a single fast-filled group, but a
     *         round completion can flush several partially-filled groups at once.
     */
    synchronized List<GroupBatch> offer(GridTaskHandler handler) {
        final TaskProcessor processor = handler.task.processor
        final TaskNode node = processorToNode.get(processor)
        if (node == null) return Collections.<GroupBatch> emptyList()

        final int level = node.getLevel()
        LevelState state = levelStates.get(level)
        if (state == null) {
            state = new LevelState()
            levelStates.put(level, state)
        }

        if (state.contributed.contains(processor)) {
            // This process already has a handler in the current round — queue this one
            // for the next round so the group's accumulated resources are never
            // double-counted for the same process.
            ArrayDeque<GridTaskHandler> queue = state.pending.get(processor)
            if (queue == null) {
                queue = new ArrayDeque<GridTaskHandler>()
                state.pending.put(processor, queue)
            }
            queue.add(handler)
            return Collections.<GroupBatch> emptyList()
        }

        final List<GroupBatch> ready = new ArrayList<>()
        admit(state, level, node, handler)
        drainReady(state, ready)
        checkRoundCompletion(state, level, ready)
        return ready
    }

    /** Place {@code node}'s handler into an open group, creating one if none fits. */
    private void admit(LevelState state, int level, TaskNode node, GridTaskHandler handler) {
        TaskGroup target = policy.placeAll([node], state.openGroups, nodeMaxCpus, nodeMaxMemory, nodeMaxTime).get(node)
        if (target == null) {
            target = new TaskGroup(level, ++groupId)
            state.openGroups.add(target)
        }
        target.addTask(node)

        List<GridTaskHandler> bucket = state.handlers.get(target)
        if (bucket == null) {
            bucket = new ArrayList<GridTaskHandler>()
            state.handlers.put(target, bucket)
        }
        bucket.add(handler)
        state.contributed.add(handler.task.processor)
    }

    /** Move any group that has reached node capacity from "open" to "ready". */
    private void drainReady(LevelState state, List<GroupBatch> ready) {
        final Iterator<TaskGroup> it = state.openGroups.iterator()
        while (it.hasNext()) {
            final TaskGroup g = it.next()
            if (isFull(g)) {
                it.remove()
                ready.add(new GroupBatch(g, state.handlers.remove(g)))
            }
        }
    }

    /**
     * When every distinct process at this level has contributed a handler to the current
     * round, flush the remaining open groups so nothing waits indefinitely, then start a
     * new round by immediately admitting any handlers that were queued while waiting.
     */
    private void checkRoundCompletion(LevelState state, int level, List<GroupBatch> ready) {
        final Integer expected = levelProcessCount.get(level)
        if (expected == null) return

        while (state.contributed.size() >= expected) {
            for (TaskGroup g : state.openGroups)
                ready.add(new GroupBatch(g, state.handlers.remove(g)))
            state.openGroups.clear()
            state.contributed.clear()

            final int admittedBefore = state.contributed.size()
            for (TaskProcessor proc : new ArrayList<TaskProcessor>(state.pending.keySet())) {
                final ArrayDeque<GridTaskHandler> queue = state.pending.get(proc)
                if (queue != null && !queue.isEmpty()) {
                    final GridTaskHandler next = queue.poll()
                    final TaskNode node = processorToNode.get(proc)
                    if (node != null) admit(state, level, node, next)
                }
            }
            drainReady(state, ready)

            // No further pending handlers were admitted — stop, remaining processes
            // simply haven't produced a handler for this round yet.
            if (state.contributed.size() == admittedBefore) break
        }
    }

    /** A group is considered full once it has saturated any capacity-constrained dimension. */
    private boolean isFull(TaskGroup group) {
        if (nodeMaxCpus > 0 && group.getTotalCpus() >= nodeMaxCpus) return true
        if (nodeMaxMemory != null && group.getTotalMemory().compareTo(nodeMaxMemory) >= 0) return true
        return false
    }

    /** Build the nodeId → workDir map for a ready batch of handlers. */
    Map<Long, Path> buildWorkDirMap(List<GridTaskHandler> handlers) {
        final Map<Long, Path> result = new HashMap<Long, Path>()
        for (GridTaskHandler h : handlers) {
            final TaskNode node = processorToNode.get(h.task.processor)
            if (node != null)
                result.put(node.getId(), h.task.workDir)
        }
        return result
    }
}
