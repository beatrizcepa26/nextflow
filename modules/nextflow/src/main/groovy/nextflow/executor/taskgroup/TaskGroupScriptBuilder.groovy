package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.analyzer.TaskNode
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.MemoryUnit

import java.nio.file.Path

/**
 * Builds a SLURM batch script for submitting a {@link TaskGroup} to a single node.
 *
 * The generated script:
 * - declares {@code #SBATCH} directives derived from the group's aggregate resource requirements
 * - allocates exactly one node ({@code -N 1}) so all tasks share the same host
 * - launches one {@code srun --exclusive} step per task in parallel (backgrounded with {@code &})
 *   so each step gets dedicated CPU cores within the allocation
 * - ends with {@code wait} to block until every step finishes
 *
 */
@Slf4j
@CompileStatic
class TaskGroupScriptBuilder {

    private static final String HEADER_TOKEN = '#SBATCH'

    /** Optional SLURM account read from executor config ({@code executor.account}). */
    private final String account
    private final TaskGroup group
    private final Map<Long, Path> taskWorkDirs

    TaskGroupScriptBuilder(TaskGroup group, Map<Long, Path> taskWorkDirs, String account) {
        this.group = group
        this.taskWorkDirs = taskWorkDirs
        this.account = account
    }

    /**
     * Build a SLURM submission script for the group.
     *
     * @return a SLURM batch script as a string
     * @throws IllegalArgumentException if a task in the group has no matching entry in {@code taskWorkDirs}
     */
    String build() {
        final script = new StringBuilder()
        script << '#!/bin/bash\n'
        script << buildHeaders()
        script << '\n'
        script << buildBody()
        return script.toString()
    }

    // header generation 
    private String buildHeaders() {
        final result = new StringBuilder()
        final directives = buildDirectives()
        for (int i = 0; i < directives.size() - 1; i += 2) {
            final String opt = directives[i]
            final String val = directives[i + 1]
            if (opt) {
                result << HEADER_TOKEN << ' ' << opt
                if (val) result << ' ' << val
                result << '\n'
            }
        }
        return result.toString()
    }

    private List<String> buildDirectives() {
        final directives = new ArrayList<String>()

        directives << '-J' << "nf-group-${group.getGroupId()}".toString()
        directives << '-N' << '1'                                       // single node
        directives << '-c' << group.getTotalCpus().toString()           // total CPUs for the allocation

        final MemoryUnit mem = group.getTotalMemory()
        if (mem && mem.compareTo(MemoryUnit.ZERO) > 0)
            directives << '--mem' << mem.toMega().toString() + 'M'

        final Duration time = group.getMaxTime()
        if (time && time.compareTo(Duration.of(0L)) > 0)
            directives << '-t' << time.format('HH:mm:ss')

        if (group.getQueue())
            directives << '-p' << group.getQueue()

        if (account)
            directives << '-A' << account

        // Discard stdout/stderr – each task's .command.run handles its own logging
        directives << '-o' << '/dev/null'
        directives << '-e' << '/dev/null'

        return directives
    }

    // body generation

    private String buildBody() {
        final body = new StringBuilder()

        for (TaskNode task : group.getTasks()) {
            final Path workDir = taskWorkDirs.get(task.getId())
            if (workDir == null)
                throw new IllegalArgumentException(
                    "No work directory provided for task '${task.getName()}' (id=${task.getId()})")

            body << "bash ${workDir}/${TaskRun.CMD_RUN} >> ${workDir}/${TaskRun.CMD_LOG} 2>&1 &\n"
        }

        body << 'wait\n'
        return body.toString()
    }
}
