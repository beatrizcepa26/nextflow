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

package nextflow.executor.analyzer

import java.nio.file.Paths

import nextflow.Session
import nextflow.dag.DAG
import nextflow.executor.GridTaskHandler
import nextflow.executor.TaskGroupCoordinator
import nextflow.executor.taskgroup.FirstFit
import nextflow.executor.taskgroup.GroupingPolicy
import nextflow.executor.taskgroup.TaskGroupBuilder
import nextflow.executor.taskgroup.Tetris
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfig
import spock.lang.Specification

/**
 * Tests for {@link SlurmTaskGroupAnalyzer#resolveGroupingPolicy}, the single place where
 * {@code executor.slurm.taskGroupingPolicy} is resolved to a {@link GroupingPolicy}.
 *
 * @author Beatriz Cepa
 */
class SlurmTaskGroupAnalyzerTest extends Specification {

    private Session sessionWithPolicy(String policyName) {
        new Session([executor: [slurm: [taskGrouping: true, taskGroupingPolicy: policyName]]])
    }

    def 'should default to first-fit when taskGroupingPolicy is unset'() {
        given:
        final session = new Session([executor: [slurm: [taskGrouping: true]]])

        expect:
        new SlurmTaskGroupAnalyzer(session).resolveGroupingPolicy() instanceof FirstFit
    }

    def 'should resolve first-fit'() {
        expect:
        new SlurmTaskGroupAnalyzer(sessionWithPolicy('first-fit')).resolveGroupingPolicy() instanceof FirstFit
    }

    def 'should resolve tetris'() {
        expect:
        new SlurmTaskGroupAnalyzer(sessionWithPolicy('tetris')).resolveGroupingPolicy() instanceof Tetris
    }

    def 'should fail fast on an unknown policy name, listing supported ones'() {
        when:
        new SlurmTaskGroupAnalyzer(sessionWithPolicy('bogus')).resolveGroupingPolicy()

        then:
        final ex = thrown(IllegalArgumentException)
        ex.message.contains('bogus')
        ex.message.contains('first-fit')
        ex.message.contains('tetris')
    }

    // --- end-to-end: the configured policy actually drives which runtime groups get produced ---

    private TaskProcessor mockProcess(int cpus) {
        final taskConfig = new TaskConfig(cpus: cpus)
        final processConfig = Mock(ProcessConfig) { createTaskConfig() >> taskConfig }
        return Mock(TaskProcessor) { getConfig() >> processConfig }
    }

    private GridTaskHandler handlerFor(TaskProcessor process) {
        final handler = new GridTaskHandler()
        handler.task = new TaskRun(processor: process, workDir: Paths.get('/work'))
        return handler
    }

    /**
     * On a 10-cpu node, A(8cpu) and B(5cpu) each open their own group (B can't fit A's group).
     * C(2cpu) fits both: First Fit takes the first group that fits (A's, filling it exactly),
     * while Tetris scores B's group higher (more slack) and picks that one instead.
     */
    private Set<String> processesGroupedWithC(String policyName) {
        final dag = new DAG()
        final procA = mockProcess(8)
        final procB = mockProcess(5)
        final procC = mockProcess(2)
        dag.addVertex(DAG.Type.PROCESS, 'A', [], [], procA)
        dag.addVertex(DAG.Type.PROCESS, 'B', [], [], procB)
        dag.addVertex(DAG.Type.PROCESS, 'C', [], [], procC)

        final session = new Session([executor: [slurm: [
            taskGrouping: true, taskGroupingPolicy: policyName, nodeCapacity: [cpus: 10]]]])
        session.@dag = dag
        final analyzer = new SlurmTaskGroupAnalyzer(session)
        final policy = analyzer.resolveGroupingPolicy()
        session.@taskGroups = new TaskGroupBuilder(analyzer, policy).build()

        final coordinator = new TaskGroupCoordinator(session, policy)
        coordinator.offer(handlerFor(procA))
        coordinator.offer(handlerFor(procB))
        final batches = coordinator.offer(handlerFor(procC))

        final label = { TaskProcessor p -> p == procA ? 'A' : (p == procB ? 'B' : 'C') }
        final withC = batches.find { b -> b.handlers.any { it.task.processor == procC } }
        return withC.handlers.collect { label(it.task.processor) } as Set
    }

    def 'changing taskGroupingPolicy from first-fit to tetris changes which groups get produced'() {
        expect:
        processesGroupedWithC('first-fit') == ['A', 'C'] as Set
        processesGroupedWithC('tetris') == ['B', 'C'] as Set
    }
}
