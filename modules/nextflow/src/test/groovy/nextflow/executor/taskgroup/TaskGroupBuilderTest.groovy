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

package nextflow.executor.taskgroup

import groovyx.gpars.dataflow.DataflowQueue
import nextflow.Session
import nextflow.dag.DAG
import nextflow.executor.analyzer.SlurmTaskGroupAnalyzer
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.script.ProcessConfig
import spock.lang.Specification

/**
 * Characterization tests for {@link TaskGroupBuilder} pinning static, DAG-level
 * First Fit grouping behavior as a baseline.
 *
 * @author Beatriz Cepa
 */
class TaskGroupBuilderTest extends Specification {

    private TaskProcessor mockProcess(int cpus) {
        final taskConfig = new TaskConfig(cpus: cpus)
        final processConfig = Mock(ProcessConfig) { createTaskConfig() >> taskConfig }
        return Mock(TaskProcessor) { getConfig() >> processConfig }
    }

    /** Adds a PROCESS vertex, optionally consuming upstream channels and/or producing one downstream. */
    private DAG.Vertex addProcess(DAG dag, String label, TaskProcessor process, List<DataflowQueue> inputs = [], DataflowQueue output = null) {
        final inbounds = inputs.collect { new DAG.ChannelHandler(channel: it, label: label) }
        final outbounds = output ? [new DAG.ChannelHandler(channel: output, label: label)] : []
        dag.addVertex(DAG.Type.PROCESS, label, inbounds, outbounds, process)
        return dag.vertices.last()
    }

    private Session sessionWith(DAG dag, Map slurmConfig = [:]) {
        final session = new Session([executor: [slurm: [taskGrouping: true] + slurmConfig]])
        session.@dag = dag
        return session
    }

    def 'should group independent processes into the same level'() {
        given: 'two independent (level 0) processes'
        final dag = new DAG()
        addProcess(dag, 'A', mockProcess(2))
        addProcess(dag, 'B', mockProcess(2))
        final session = sessionWith(dag)

        when:
        final groups = new TaskGroupBuilder(new SlurmTaskGroupAnalyzer(session), new FirstFit()).build()

        then:
        groups.keySet() == [0] as Set
        groups[0].size() == 1
        groups[0][0].getSize() == 2
        groups[0][0].getLevel() == 0
    }

    def 'should assign a downstream process to the next level'() {
        given: 'B depends on A'
        final dag = new DAG()
        final chA = new DataflowQueue()
        addProcess(dag, 'A', mockProcess(1), [], chA)
        addProcess(dag, 'B', mockProcess(1), [chA])
        final session = sessionWith(dag)

        when:
        final groups = new TaskGroupBuilder(new SlurmTaskGroupAnalyzer(session), new FirstFit()).build()

        then:
        groups.keySet() == [0, 1] as Set
        groups[0][0].getTasks()*.getName() == ['A']
        groups[1][0].getTasks()*.getName() == ['B']
    }

    def 'should open a new group once node CPU capacity is exceeded'() {
        given: 'three same-level processes that cannot all fit in one 4-cpu group'
        final dag = new DAG()
        addProcess(dag, 'A', mockProcess(2))
        addProcess(dag, 'B', mockProcess(2))
        addProcess(dag, 'C', mockProcess(2))
        final session = sessionWith(dag, [nodeCapacity: [cpus: 4]])

        when:
        final groups = new TaskGroupBuilder(new SlurmTaskGroupAnalyzer(session), new FirstFit()).build()

        then:
        groups[0].size() == 2
        groups[0]*.getTotalCpus().sum() == 6
        groups[0].every { it.getTotalCpus() <= 4 }
    }

    def 'should fail fast when a single task exceeds node capacity'() {
        given:
        final dag = new DAG()
        addProcess(dag, 'A', mockProcess(8))
        final session = sessionWith(dag, [nodeCapacity: [cpus: 4]])

        when:
        new TaskGroupBuilder(new SlurmTaskGroupAnalyzer(session), new FirstFit()).build()

        then:
        thrown(IllegalStateException)
    }
}
