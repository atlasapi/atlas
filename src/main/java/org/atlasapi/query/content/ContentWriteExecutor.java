package org.atlasapi.query.content;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.query.content.merge.BroadcastMerger;
import org.atlasapi.query.content.merge.ContentMerger;
import org.atlasapi.query.content.merge.DefaultBroadcastMerger;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteExecutor {

    private final ModelReader reader;
    private final ModelTransformer<Description, Content> transformer;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final EventResolver eventResolver;
    private final ContentMerger contentMerger;
    private final BroadcastMerger broadcastMerger;

    private ContentWriteExecutor(
            ModelReader reader,
            ModelTransformer<Description, Content> transformer,
            ContentResolver resolver,
            ContentWriter writer,
            ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver,
            EventResolver eventResolver,
            ContentMerger contentMerger,
            BroadcastMerger broadcastMerger
    ) {
        this.reader = checkNotNull(reader);
        this.transformer = checkNotNull(transformer);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.eventResolver = checkNotNull(eventResolver);
        this.contentMerger = checkNotNull(contentMerger);
        this.broadcastMerger = checkNotNull(broadcastMerger);
    }

    public static ContentWriteExecutor create(
            ModelReader reader,
            ModelTransformer<Description, Content> transformer,
            ContentResolver resolver,
            ContentWriter writer,
            ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver,
            EventResolver eventResolver,
            ContentMerger contentMerger,
            BroadcastMerger broadcastMerger
    ) {
        return new ContentWriteExecutor(
                reader,
                transformer,
                resolver,
                writer,
                scheduleWriter,
                channelResolver,
                eventResolver,
                contentMerger,
                broadcastMerger
        );
    }

    public InputContent parseInputStream(
            InputStream inputStream, Boolean strict
    ) throws IOException, ReadException {
        Description description = deserialize(new InputStreamReader(inputStream), strict);
        Content content = complexify(description);

        return InputContent.create(content, description.getType());
    }

    public void writeContent(Content content, String type, boolean shouldMerge) {
        writeContentInternal(content, type, shouldMerge, DefaultBroadcastMerger.defaultMerger());
    }

    public void writeContent(
            Content content,
            String type,
            boolean shouldMerge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ) {
        writeContentInternal(content, type, shouldMerge, defaultBroadcastMerger);
    }

    private void writeContentInternal(
            Content content,
            String type,
            boolean shouldMerge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ) {
        checkArgument(content.getId() != null, "Cannot write content without an ID");

        Content updatedContent = updateEventPublisher(content);

        Maybe<Identified> identified = resolveExisting(updatedContent);

        if ("broadcast".equals(type)) {
            updatedContent = broadcastMerger.mergeBroadcasts(
                    identified, updatedContent, shouldMerge, defaultBroadcastMerger
            );
        } else {
            updatedContent = contentMerger.merge(
                    identified, updatedContent, shouldMerge, defaultBroadcastMerger
            );
        }
        if (updatedContent instanceof Item) {
            Item item = (Item) updatedContent;
            writer.createOrUpdate(item);
            updateSchedule(item);
        } else {
            writer.createOrUpdate((Container) updatedContent);
        }
    }

    private Description deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(new BufferedReader(input), Description.class, strict);
    }

    private Content complexify(Description inputContent) {
        return transformer.transform(inputContent);
    }

    private Content updateEventPublisher(Content content) {
        List<EventRef> eventRefs = content.events();
        for (EventRef eventRef : eventRefs) {
            Event event = eventResolver.fetch(eventRef.id()).orNull();
            checkNotNull(event);
            checkNotNull(event.publisher());
            eventRef.setPublisher(event.publisher());
        }
        content.setEventRefs(ImmutableList.copyOf(eventRefs));
        return content;
    }

    private Maybe<Identified> resolveExisting(Content content) {
        ImmutableSet<String> uris = ImmutableSet.of(content.getCanonicalUri());
        ResolvedContent resolved = resolver.findByCanonicalUris(uris);
        return resolved.get(content.getCanonicalUri());
    }

    private void updateSchedule(Item item) {
        Iterable<Broadcast> broadcasts = Iterables.concat(Iterables.transform(item.getVersions(),
                Version.TO_BROADCASTS));
        for (Broadcast broadcast : broadcasts) {
            Maybe<Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
            if (channel.hasValue()) {
                scheduleWriter.replaceScheduleBlock(item.getPublisher(),
                        channel.requireValue(),
                        ImmutableSet.of(new ScheduleEntry.ItemRefAndBroadcast(item, broadcast)));
            }
        }
    }

    public static class InputContent {

        private final Content content;
        private final String type;

        private InputContent(Content content, String type) {
            this.content = content;
            this.type = type;
        }

        public static InputContent create(Content content, String type) {
            return new InputContent(content, type);
        }

        public Content getContent() {
            return content;
        }

        public String getType() {
            return type;
        }
    }
}
