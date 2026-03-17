package nextflow.executor.taskgroup

import groovy.transform.CompileStatic
import nextflow.executor.analyzer.TaskNode
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import java.util.stream.Collectors

/**
 * Represents a group of tasks that can be submitted in one batch script.
 *
 * A group corresponds to a level in the Dependency Graph and accumulates resource requirements
 * for all enclosed tasks.
 *
 * @author Beatriz Cepa
 */
@CompileStatic
class TaskGroup {

	private final int level
	private final int groupId
	private final List<TaskNode> tasks 

	private int totalCpus
	private MemoryUnit totalMemory = MemoryUnit.ZERO
	private Duration maxTime = Duration.of(0L)
	private String queue

	TaskGroup(int level, int groupId) {
		this.level = level
		this.groupId = groupId
		this.tasks = new ArrayList<>()
	}

	int getLevel() { level }

	int getGroupId() { groupId }

	List<TaskNode> getTasks() { tasks }

	int getSize() { tasks.size() }

	int getTotalCpus() { totalCpus }

	MemoryUnit getTotalMemory() { totalMemory }

	Duration getMaxTime() { maxTime }

	String getQueue() { queue }

	List<Long> getTaskIds() {
		this.tasks.stream()
			.map {TaskNode::getId}
			.collect(Collectors.toList())
	}

	/**
	 * Add a task and update aggregate group resources.
	 */
	void addTask(TaskNode task) {
		if( task == null )
			throw new IllegalArgumentException("Task cannot be null")

		if( task.getLevel() != level ) {
			throw new IllegalArgumentException("Task level ${task.getLevel()} does not match group level ${level}")
		}

		if( queue == null )
			queue = task.getQueue()
		else if( task.getQueue() != queue )
			throw new IllegalArgumentException("Task queue '${task.getQueue()}' does not match group queue '${queue}'")

		tasks.add(task)
		totalCpus += task.getCpus()

		if( task.getMemory() != null )
			totalMemory = totalMemory.plus(task.getMemory())

		if( task.getTime() != null && task.getTime().compareTo(maxTime) > 0 )
			maxTime = task.getTime()
	}
}
