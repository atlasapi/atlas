package org.atlasapi.output;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.feeds.tasks.Task;
import org.atlasapi.feeds.tasks.simple.TaskQueryResult;
import org.atlasapi.output.simple.ModelSimplifier;

import java.util.Set;

public class SimpleTaskModelWriter extends TransformingModelWriter<Iterable<Task>, TaskQueryResult> {

    private final ModelSimplifier<Task, org.atlasapi.feeds.tasks.simple.Task> taskSimplifier;

    public SimpleTaskModelWriter(
            AtlasModelWriter<TaskQueryResult> delegate,
            ModelSimplifier<Task, org.atlasapi.feeds.tasks.simple.Task> transactionSimplifier
    ) {
        super(delegate);
        this.taskSimplifier = transactionSimplifier;
    }
    
    @Override
    protected TaskQueryResult transform(
            Iterable<Task> fullTasks,
            Set<Annotation> annotations,
            Application application
    ) {
        TaskQueryResult result = new TaskQueryResult();
        for (Task fullTask : fullTasks) {
            result.add(taskSimplifier.simplify(fullTask, annotations, application));
        }
        return result;
    }

}
