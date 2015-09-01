package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.tasks.Task;
import org.atlasapi.feeds.tasks.simple.TaskQueryResult;
import org.atlasapi.output.simple.ModelSimplifier;

public class SimpleTaskModelWriter extends TransformingModelWriter<Iterable<Task>, TaskQueryResult> {

    private final ModelSimplifier<Task, org.atlasapi.feeds.tasks.simple.Task> taskSimplifier;

    public SimpleTaskModelWriter(AtlasModelWriter<TaskQueryResult> delegate, ModelSimplifier<Task, org.atlasapi.feeds.tasks.simple.Task> transactionSimplifier) {
        super(delegate);
        this.taskSimplifier = transactionSimplifier;
    }
    
    @Override
    protected TaskQueryResult transform(Iterable<Task> fullTasks, Set<Annotation> annotations, ApplicationConfiguration config) {
        TaskQueryResult result = new TaskQueryResult();
        for (Task fullTask : fullTasks) {
            result.add(taskSimplifier.simplify(fullTask, annotations, config));
        }
        return result;
    }

}
