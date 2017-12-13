package org.atlasapi.reporting;

import com.metabroadcast.status.api.*;

public class Utils {
    public static PartialStatus getMissingContentTitleStatus(
            String contentType,
            String contentId,
            String taskId,
            boolean isOk
    ) {
        Category category = Category.ALERT;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The title is missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getMissingContentTitleStatus(
            String contentType,
            long contentId,
            String taskId,
            boolean isOk
    ) {

        Category category = Category.ALERT;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The title is missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getMissingContentGenresStatus(
            String contentType,
            String contentId,
            String taskId,
            boolean isOk
    ) {

        Category category = Category.TO_SET;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.GENRE)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The genres are missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getMissingContentGenresStatus(
            String contentType,
            long contentId,
            String taskId,
            boolean isOk
    ) {

        Category category = Category.TO_SET;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.GENRE)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The genres are missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getMissingEpisodeNumberStatus(
            String contentType,
            String contentId,
            String taskId,
            boolean isOk
    ) {

        Category category = Category.ALERT;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.EPISODE_NUMBER)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The episode number is missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }

    public static PartialStatus getMissingEpisodeNumberStatus(
            String contentType,
            long contentId,
            String taskId,
            boolean isOk
    ) {

        Category category = Category.ALERT;
        if (isOk){
            category = Category.AUTO_ASSIGNED;
        }

        return PartialStatus.builder()
                .withAlert(NewAlert.builder()
                        .withKey(
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.EPISODE_NUMBER)
                        .withCategory(category)
                        .withValue("")
                        .withDescription(
                                String.format("The episode number is missing " +
                                                "for the given %s: %s",
                                        contentType,
                                        contentId))
                        .build())
                .withEntity(EntityRef.create(
                        EntityRef.Type.CONTENT,
                        contentId))
                .withTask(TaskRef.builder()
                        .withId(taskId)
                        .build())
                .build();
    }
}
