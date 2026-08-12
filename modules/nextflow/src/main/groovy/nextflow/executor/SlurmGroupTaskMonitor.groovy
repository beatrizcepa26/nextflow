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
import nextflow.exception.ProcessNonZeroExitStatusException
import nextflow.executor.taskgroup.TaskGroup
import nextflow.processor.TaskHandler
import nextflow.processor.TaskPollingMonitor
import nextflow.util.Duration

import java.nio.file.Path

/**
 * A {@link TaskPollingMonitor} that intercepts tasks belonging to a {@link TaskGroup} and
 * submits them as a single SLURM batch job via {@link SlurmExecutor#buildGroupScript}.
 *
 * <p>Handlers for grouped tasks are buffered in a {@link TaskGroupCoordinator}. When the
 * coordinator signals that a complete batch is ready (one handler per distinct process in the
 * group), this monitor:</p>
 * <ol>
 *   <li>Builds the group SBATCH script.</li>
 *   <li>Pipes it to {@code sbatch} stdin and captures the job ID.</li>
 *   <li>Assigns that job ID to every handler in the batch and adds them to the running queue.</li>
 * </ol>
 *
 * <p>Non-grouped tasks are handled by the default {@link TaskPollingMonitor#submit} path.</p>
 *
 * <p>The {@link TaskGroupCoordinator} is initialised lazily on the first {@link #submit} call,
 * at which point {@code session.taskGroups} is guaranteed to be populated (it is built during
 * {@code Session.notifyFlowBegin()}, before any task runs are submitted).</p>
 *
 * @author Beatriz Cepa
 */
@Slf4j
@CompileStatic
class SlurmGroupTaskMonitor extends TaskPollingMonitor {

    private final SlurmExecutor slurmExecutor

    /** Lazily initialised once session.taskGroups is available. */
    private volatile TaskGroupCoordinator coordinator

    protected SlurmGroupTaskMonitor(SlurmExecutor executor, Map params) {
        super(params)
        this.slurmExecutor = executor
    }

    static SlurmGroupTaskMonitor create(SlurmExecutor executor) {
        final sess = executor.session
        final cfg  = executor.config
        final nm   = executor.name
        final capacity     = cfg.getQueueSize(nm, 100)
        final pollInterval = cfg.getPollInterval(nm, Duration.of('5 sec'))
        final dumpInterval = cfg.getMonitorDumpInterval(nm)
        return new SlurmGroupTaskMonitor(executor, [
            name: nm, session: sess, config: cfg,
            capacity: capacity, pollInterval: pollInterval, dumpInterval: dumpInterval
        ])
    }

    private synchronized TaskGroupCoordinator getCoordinator() {
        if (coordinator == null)
            coordinator = new TaskGroupCoordinator(session)
        return coordinator
    }

    /**
     * Intercept scheduling: grouped tasks are buffered directly in the coordinator and never
     * enter the pending queue, so the standard submit loop does not stall waiting for them.
     * Non-grouped tasks follow the normal path.
     */
    @Override
    void schedule(TaskHandler handler) {
        if (!(handler instanceof GridTaskHandler)) {
            super.schedule(handler)
            return
        }

        final gridHandler = (GridTaskHandler) handler
        final coord = getCoordinator()

        if (!coord.isGrouped(gridHandler.task)) {
            super.schedule(handler)
            return
        }

        // Notify pending so trace/observers see the task enter the system.
        notifyTaskPending(handler)

        // Prepare the individual task wrapper (.command.run, .command.sh etc.) before buffering.
        gridHandler.prepareLauncher()
        log.debug "[SLURM TASK GROUPING] Buffering task '${gridHandler.task.name}' — waiting for group to complete"

        final List<TaskGroupCoordinator.GroupBatch> batches = coord.offer(gridHandler)
        for (TaskGroupCoordinator.GroupBatch batch : batches)
            submitGroup(batch.group, batch.handlers)
    }

    /** Build and submit the SBATCH script for a single ready {@link TaskGroup} batch. */
    private void submitGroup(TaskGroup group, List<GridTaskHandler> ready) {
        final Map<Long, Path> workDirs = getCoordinator().buildWorkDirMap(ready)
        final String script = slurmExecutor.buildGroupScript(group, workDirs)
        final Path scriptFile = ready[0].task.workDir.resolve('.command.group')
        scriptFile.text = script

        log.debug "[SLURM TASK GROUPING] Submitting group ${group.groupId} (${ready.size()} task(s)); script: ${scriptFile}"
        try {
            final String result = submitGroupScript(scriptFile)
            final String jobId  = (String) slurmExecutor.parseJobId(result)
            log.debug "[SLURM TASK GROUPING] Group ${group.groupId} submitted > jobId: $jobId"
            for (GridTaskHandler h : ready) {
                h.updateStatus(jobId)
                // Mark the handler as RUNNING immediately: for grouped tasks the group SLURM job is
                // already submitted and executing, so we don't need to poll for .command.start or
                // wait for the job to appear in squeue. Skipping isStarted() prevents a permanent
                // stall when the job finishes quickly and neither file is NFS-visible within the
                // 270 s check window.
                h.markAsStarted()
                getRunningQueue().add(h)
                session.notifyTaskSubmit(h)
                session.notifyTaskStart(h)
                log.debug "[SLURM TASK GROUPING] Task '${h.task.name}' marked as started > jobId: $jobId"
            }
        }
        catch (Exception e) {
            log.error "[SLURM TASK GROUPING] Failed to submit group ${group.groupId}: ${e.message}", e
            throw e
        }
    }

    @Override
    protected void submit(TaskHandler handler) {
        // Grouped tasks are handled entirely in schedule(); this path is only reached for
        // non-grouped tasks that went through super.schedule() → pendingQueue → submitLoop.
        super.submit(handler)
    }

    private String submitGroupScript(Path scriptFile) {
        final process = new ProcessBuilder(['sbatch', scriptFile.toString()])
            .redirectErrorStream(true)
            .start()
        try {
            final result = process.text
            final exit   = process.waitFor()
            if (exit != 0)
                throw new ProcessNonZeroExitStatusException(
                    "Failed to submit task group to SLURM", result, exit, 'sbatch')
            return result
        }
        finally {
            process.in.closeQuietly()
            process.out.closeQuietly()
            process.err.closeQuietly()
            process.destroy()
        }
    }
}
