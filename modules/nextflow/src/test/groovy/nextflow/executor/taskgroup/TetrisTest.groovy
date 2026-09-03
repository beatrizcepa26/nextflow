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

import nextflow.dag.DAG
import nextflow.executor.analyzer.TaskNode
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.script.ProcessConfig
import nextflow.util.MemoryUnit
import spock.lang.Specification

/**
 * Unit tests for the {@link Tetris} scoring/placement heuristic in isolation.
 *
 * @author Beatriz Cepa
 */
class TetrisTest extends Specification {

    private final DAG dag = new DAG()

    private TaskNode taskNode(String name, int cpus, String memory = null, String queue = null) {
        final taskConfig = new TaskConfig(cpus: cpus, memory: memory, queue: queue)
        final processConfig = Mock(ProcessConfig) { createTaskConfig() >> taskConfig }
        final process = Mock(TaskProcessor) { getConfig() >> processConfig }
        dag.addVertex(DAG.Type.PROCESS, name, [], [], process)
        return new TaskNode(dag.vertices.last(), process)
    }

    private TaskGroup groupWith(List<TaskNode> preloaded = []) {
        final group = new TaskGroup(0, preloaded.size() + 1)
        preloaded.each { group.addTask(it) }
        return group
    }

    def 'should place a task into a group it fits'() {
        given:
        final group = groupWith([taskNode('used', 2)])
        final task = taskNode('T', 2)

        expect:
        new Tetris().placeAll([task], [group], 10, null, null) == [(task): group]
    }

    def 'should leave a task unplaced when no open group has enough remaining cpus'() {
        given:
        final group = groupWith([taskNode('used', 8)])
        final task = taskNode('T', 5)

        expect:
        new Tetris().placeAll([task], [group], 10, null, null) == [(task): null]
    }

    def 'should leave a task unplaced when no open group has enough remaining memory'() {
        given:
        final group = groupWith([taskNode('used', 1, '3 GB')])
        final task = taskNode('T', 1, '2 GB')

        expect:
        new Tetris().placeAll([task], [group], 0, MemoryUnit.of('4 GB'), null) == [(task): null]
    }

    def 'should skip a group whose queue does not match the task queue'() {
        given:
        final group = groupWith([taskNode('used', 1, null, 'alpha')])
        final task = taskNode('T', 1, null, 'beta')

        expect:
        new Tetris().placeAll([task], [group], 10, null, null) == [(task): null]
    }

    def 'should never place tasks beyond the node cpu ceiling'() {
        given: 'three 3-cpu tasks competing for a single 6-cpu group'
        final group = groupWith()
        final tasks = [taskNode('a', 3), taskNode('b', 3), taskNode('c', 3)]

        when:
        final result = new Tetris().placeAll(tasks, [group], 6, null, null)

        then:
        result.values().findAll { it == group }.size() == 2
        result.values().count { it == null } == 1
        tasks.findAll { result[it] == group }*.getCpus().sum() <= 6
    }

    def 'should never place tasks beyond the node memory ceiling'() {
        given: 'three 2GB tasks competing for a single 4GB group'
        final group = groupWith()
        final tasks = [taskNode('a', 1, '2 GB'), taskNode('b', 1, '2 GB'), taskNode('c', 1, '2 GB')]

        when:
        final result = new Tetris().placeAll(tasks, [group], 0, MemoryUnit.of('4 GB'), null)

        then:
        result.values().findAll { it == group }.size() == 2
        result.values().count { it == null } == 1
    }

    def 'should assign the highest-scoring pair first, then re-check feasibility for the rest'() {
        given: 'G1 has 3 cpus remaining, G2 has 6 remaining, out of a 10-cpu node'
        final g1 = groupWith([taskNode('g1-used', 7)])
        final g2 = groupWith([taskNode('g2-used', 4)])
        final small = taskNode('small', 3) // best score against G2 (18) beats its score against G1 (9)
        final big = taskNode('big', 6)     // only fits G2, with the single highest score (36)

        when: 'big grabs G2 first, which then leaves no room there for small'
        final result = new Tetris().placeAll([small, big], [g1, g2], 10, null, null)

        then:
        result[big] == g2
        result[small] == g1
    }
}
