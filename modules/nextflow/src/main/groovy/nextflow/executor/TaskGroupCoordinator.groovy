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
import nextflow.executor.analyzer.TaskNode
import nextflow.executor.taskgroup.TaskGroup
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun

import java.nio.file.Path

/**
 * Coordinates the buffering and batching of {@link GridTaskHandler}s that belong to the same
 * {@link TaskGroup}.
 *
 * Each {@link TaskGroup} contains one {@link TaskNode} per process (DAG vertex). At runtime, each
 * process emits multiple {@link TaskRun}s (one per input item). This coordinator accumulates
 * handlers until it has exactly one handler for every distinct process in the group, then returns
 * that batch for group submission. Subsequent runs of the same processes form the next batch.
 *
 * @author Beatriz Cepa
 */
@Slf4j
@CompileStatic
class TaskGroupCoordinator {

    private final Session session

    /** Maps TaskProcessor → the TaskGroup it belongs to. */
    private final Map<TaskProcessor, TaskGroup> processorToGroup = new HashMap<>()

    /** Maps TaskProcessor → its TaskNode id within the group (used as workDir key). */
    private final Map<TaskProcessor, Long> processorToNodeId = new HashMap<>()

    /** Expected number of distinct processes (TaskNodes) per group. */
    private final Map<TaskGroup, Integer> groupExpectedSize = new HashMap<>()

    /**
     * Per-group, per-nodeId queue of buffered handlers.
     * Allows multiple task runs for the same process to queue up independently.
     */
    private final Map<TaskGroup, Map<Long, ArrayDeque<GridTaskHandler>>> pendingQueues = new HashMap<>()

    TaskGroupCoordinator(Session session) {
        this.session = session
        buildLookups()
    }

    private void buildLookups() {
        final Map<Long, TaskGroup> nodeIdToGroup = new HashMap<>()
        for (List<TaskGroup> groups : session.getTaskGroups().values()) {
            for (TaskGroup group : groups) {
                groupExpectedSize.put(group, group.getSize())
                for (TaskNode node : group.getTasks())
                    nodeIdToGroup.put(node.getId(), group)
            }
        }

        if (session.dag == null) return

        for (DAG.Vertex vertex : session.dag.vertices) {
            if (vertex.type == DAG.Type.PROCESS && vertex.process != null) {
                final TaskGroup group = nodeIdToGroup.get(vertex.id)
                if (group != null) {
                    processorToGroup.put(vertex.process, group)
                    processorToNodeId.put(vertex.process, vertex.id)
                    log.debug "[SLURM TASK GROUPING] Process '${vertex.label}' mapped to group ${group.getGroupId()}"
                }
            }
        }
        log.debug "[SLURM TASK GROUPING] Coordinator initialised: ${processorToGroup.size()} processes across ${groupExpectedSize.size()} group(s)"
    }

    boolean isGrouped(TaskRun task) {
        return processorToGroup.containsKey(task.processor)
    }

    TaskGroup findGroup(TaskRun task) {
        return processorToGroup.get(task.processor)
    }

    /**
     * Accept a handler for a grouped task.
     *
     * Returns a complete batch — one handler per distinct TaskNode in the group — when enough
     * handlers have accumulated, or {@code null} if still waiting for more processes to contribute.
     */
    synchronized List<GridTaskHandler> offer(GridTaskHandler handler) {
        final TaskGroup group = processorToGroup.get(handler.task.processor)
        if (group == null) return null

        final Long nodeId = processorToNodeId.get(handler.task.processor)

        if (!pendingQueues.containsKey(group))
            pendingQueues.put(group, new LinkedHashMap<Long, ArrayDeque<GridTaskHandler>>())
        final Map<Long, ArrayDeque<GridTaskHandler>> queues = pendingQueues.get(group)

        if (!queues.containsKey(nodeId))
            queues.put(nodeId, new ArrayDeque<GridTaskHandler>())
        queues.get(nodeId).add(handler)

        // A batch is ready when every distinct process (nodeId) in the group has contributed one handler.
        if (queues.size() < groupExpectedSize.get(group)) return null
        for (ArrayDeque<GridTaskHandler> q : queues.values())
            if (q.isEmpty()) return null

        final List<GridTaskHandler> batch = new ArrayList<GridTaskHandler>()
        for (ArrayDeque<GridTaskHandler> q : queues.values())
            batch.add(q.poll())
        return batch
    }

    /** Build the nodeId → workDir map for a ready batch of handlers. */
    Map<Long, Path> buildWorkDirMap(List<GridTaskHandler> handlers) {
        final Map<Long, Path> result = new HashMap<Long, Path>()
        for (GridTaskHandler h : handlers) {
            final Long nodeId = processorToNodeId.get(h.task.processor)
            if (nodeId != null)
                result.put(nodeId, h.task.workDir)
        }
        return result
    }
}
