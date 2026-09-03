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

import java.nio.file.Path
import java.nio.file.Paths

import nextflow.Session
import nextflow.dag.DAG
import nextflow.executor.analyzer.SlurmTaskGroupAnalyzer
import nextflow.executor.taskgroup.FirstFit
import nextflow.executor.taskgroup.TaskGroupBuilder
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfig
import spock.lang.Specification

/**
 * Characterization tests for {@link TaskGroupCoordinator}, pinning runtime, per-level
 * buffering behavior (First Fit) as a baseline.
 *
 * @author Beatriz Cepa
 */
class TaskGroupCoordinatorTest extends Specification {

    private TaskProcessor mockProcess(int cpus) {
        final taskConfig = new TaskConfig(cpus: cpus)
        final processConfig = Mock(ProcessConfig) { createTaskConfig() >> taskConfig }
        return Mock(TaskProcessor) { getConfig() >> processConfig }
    }

    private void addProcess(DAG dag, String label, TaskProcessor process) {
        dag.addVertex(DAG.Type.PROCESS, label, [], [], process)
    }

    private GridTaskHandler handlerFor(TaskProcessor process, Path workDir = Paths.get('/work')) {
        final handler = new GridTaskHandler()
        handler.task = new TaskRun(processor: process, workDir: workDir)
        return handler
    }

    /** Builds a session with a static plan already computed by {@link TaskGroupBuilder}, as production does. */
    private Session sessionWith(DAG dag, Map slurmConfig = [:]) {
        final session = new Session([executor: [slurm: [taskGrouping: true] + slurmConfig]])
        session.@dag = dag
        final builder = new TaskGroupBuilder(new SlurmTaskGroupAnalyzer(session), new FirstFit())
        session.@taskGroups = builder.build()
        return session
    }

    def 'offer() should buffer a handler until node capacity is reached'() {
        given: 'two same-level processes that together saturate a 4-cpu node'
        final dag = new DAG()
        final procA = mockProcess(2)
        final procB = mockProcess(2)
        addProcess(dag, 'A', procA)
        addProcess(dag, 'B', procB)
        final session = sessionWith(dag, [nodeCapacity: [cpus: 4]])
        final coordinator = new TaskGroupCoordinator(session, new FirstFit())

        expect:
        coordinator.offer(handlerFor(procA)).isEmpty()
    }

    def 'offer() should flush the group immediately once it reaches node capacity'() {
        given:
        final dag = new DAG()
        final procA = mockProcess(2)
        final procB = mockProcess(2)
        addProcess(dag, 'A', procA)
        addProcess(dag, 'B', procB)
        final session = sessionWith(dag, [nodeCapacity: [cpus: 4]])
        final coordinator = new TaskGroupCoordinator(session, new FirstFit())
        coordinator.offer(handlerFor(procA))

        when:
        final batches = coordinator.offer(handlerFor(procB))

        then:
        batches.size() == 1
        batches[0].group.getTotalCpus() == 4
        batches[0].handlers.size() == 2
    }

    def 'offer() should flush partially-filled groups once every process in the level has contributed'() {
        given: 'three same-level processes that never saturate a 10-cpu node'
        final dag = new DAG()
        final procA = mockProcess(1)
        final procB = mockProcess(1)
        final procC = mockProcess(1)
        addProcess(dag, 'A', procA)
        addProcess(dag, 'B', procB)
        addProcess(dag, 'C', procC)
        final session = sessionWith(dag, [nodeCapacity: [cpus: 10]])
        final coordinator = new TaskGroupCoordinator(session, new FirstFit())
        coordinator.offer(handlerFor(procA))
        coordinator.offer(handlerFor(procB))

        when:
        final batches = coordinator.offer(handlerFor(procC))

        then: 'the round completes even though the group never reached node capacity'
        batches.size() == 1
        batches[0].group.getTotalCpus() == 3
        batches[0].handlers.size() == 3
    }

    def 'isGrouped() should report true only for processes covered by the static plan'() {
        given:
        final dag = new DAG()
        final procA = mockProcess(1)
        addProcess(dag, 'A', procA)
        final session = sessionWith(dag)
        final coordinator = new TaskGroupCoordinator(session, new FirstFit())

        expect:
        coordinator.isGrouped(new TaskRun(processor: procA))
        !coordinator.isGrouped(new TaskRun(processor: Mock(TaskProcessor)))
    }

    def 'buildWorkDirMap() should map each grouped node id to its handler work directory'() {
        given:
        final dag = new DAG()
        final procA = mockProcess(2)
        final procB = mockProcess(2)
        addProcess(dag, 'A', procA)
        addProcess(dag, 'B', procB)
        final session = sessionWith(dag, [nodeCapacity: [cpus: 4]])
        final coordinator = new TaskGroupCoordinator(session, new FirstFit())
        final workDirA = Paths.get('/work/a')
        final workDirB = Paths.get('/work/b')
        coordinator.offer(handlerFor(procA, workDirA))
        final batches = coordinator.offer(handlerFor(procB, workDirB))

        when:
        final workDirMap = coordinator.buildWorkDirMap(batches[0].handlers)

        then:
        workDirMap.size() == 2
        workDirMap.values() as Set == [workDirA, workDirB] as Set
    }
}
