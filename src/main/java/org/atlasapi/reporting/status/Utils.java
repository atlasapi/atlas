package org.atlasapi.reporting.status;


import com.metabroadcast.status.api.EntityAndPublisher;
import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.NewAlert;
import com.metabroadcast.status.api.PartialStatus;
import com.metabroadcast.status.api.State;
import com.metabroadcast.status.api.TaskRef;

public class Utils {

    public static PartialStatus getPartialStatusForContent(
            String id,
            String taskId,
            NewAlert.Key.Check alertCheck,
            NewAlert.Key.Field alertField,
            String description,
            EntityRef.Type entityRefType,
            String publisher,
            boolean isOk
    ) {
        State state = isOk ? State.AUTO_CLEARED : State.FIRING;

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(alertCheck,alertField)
                        .withState(state)
                        .withValue("")
                        .withDescription(description!=null?description:"")
                        .build())
                .withEntity(EntityAndPublisher.create(entityRefType, id, publisher))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getPartialStatusForContent(
            long id,
            String taskId,
            NewAlert.Key.Check alertCheck,
            NewAlert.Key.Field alertField,
            String description,
            EntityRef.Type entityRefType,
            String publisher,
            boolean isOk
    ) {
        State state = isOk ? State.AUTO_CLEARED : State.FIRING;

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(alertCheck,alertField)
                        .withState(state)
                        .withValue("")
                        .withDescription(description!=null?description:"")
                        .build())
                .withEntity(EntityAndPublisher.create(entityRefType, id, publisher))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }
}
