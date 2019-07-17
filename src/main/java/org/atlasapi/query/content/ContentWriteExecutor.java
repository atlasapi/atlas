package org.atlasapi.query.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
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
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.EquivalenceContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.query.content.merge.BroadcastMerger;
import org.atlasapi.query.content.merge.ContentMerger;
import org.atlasapi.query.content.merge.VersionMerger;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteExecutor {

    private static final Logger log = LoggerFactory.getLogger(ContentWriteExecutor.class);

    private final ModelReader reader;
    private final ModelTransformer<Description, Content> transformer;
    private final ContentResolver resolver;
    private final EquivalenceContentWriter writer;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final EventResolver eventResolver;
    private final ContentMerger contentMerger;
    private final VersionMerger versionMerger;
    private final BroadcastTrimmer trimmer;

    private ContentWriteExecutor(
            ModelReader reader,
            ModelTransformer<Description, Content> transformer,
            ContentResolver resolver,
            EquivalenceContentWriter writer,
            ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver,
            EventResolver eventResolver,
            ContentMerger contentMerger,
            VersionMerger versionMerger,
            BroadcastTrimmer trimmer
    ) {
        this.reader = checkNotNull(reader);
        this.transformer = checkNotNull(transformer);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.eventResolver = checkNotNull(eventResolver);
        this.contentMerger = checkNotNull(contentMerger);
        this.versionMerger = checkNotNull(versionMerger);
        this.trimmer = checkNotNull(trimmer);
    }

    public static ContentWriteExecutor create(
            ModelReader reader,
            ModelTransformer<Description, Content> transformer,
            ContentResolver resolver,
            EquivalenceContentWriter writer,
            ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver,
            EventResolver eventResolver,
            ContentMerger contentMerger,
            VersionMerger versionMerger,
            BroadcastTrimmer trimmer
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
                versionMerger,
                trimmer
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
        writeContentInternal(content, type, shouldMerge, BroadcastMerger.defaultMerger());
    }

    public void writeContent(
            Content content,
            String type,
            boolean shouldMerge,
            BroadcastMerger broadcastMerger
    ) {
        writeContentInternal(content, type, shouldMerge, broadcastMerger);
    }

    private void writeContentInternal(
            Content content,
            String type,
            boolean shouldMerge,
            BroadcastMerger broadcastMerger
    ) {
        checkArgument(content.getId() != null, "Cannot write content without an ID");

        Content updatedContent = updateEventPublisher(content);
        Maybe<Identified> identified = resolveExisting(updatedContent);

        if ("broadcast".equals(type)) {
            updatedContent = versionMerger.mergeBroadcasts(
                    identified, updatedContent, shouldMerge, broadcastMerger
            );
        } else {
            updatedContent = contentMerger.merge(
                    identified, updatedContent, shouldMerge, broadcastMerger
            );
        }
        long startTime = System.nanoTime();
        if (updatedContent instanceof Item) {
            Item item = (Item) updatedContent;
            writer.createOrUpdate(item);
            updateSchedule(content, item);
        } else {
            writer.createOrUpdate((Container) updatedContent);
        }

        long duration = (System.nanoTime() - startTime)/1000000;
        if(duration > 1000){
            log.info("TIMER. Super slow. Writing to db took {}ms. uri={} {}",duration, content.getCanonicalUri(), Thread.currentThread().getName());
        }
    }

    private void updateSchedule(Content content, Item item) {
        for (Broadcast broadcast : content.getVersions()
                .iterator()
                .next()
                .getBroadcasts()) {
            Interval broadcastInterval = new Interval(
                    broadcast.getTransmissionTime(),
                    broadcast.getTransmissionEndTime()
            );

            Channel channel = channelResolver.fromUri(broadcast.getBroadcastOn())
                    .requireValue();

            ImmutableMap<String, String> acceptableIds = ImmutableMap.of(
                    broadcast.getSourceId(),
                    item.getCanonicalUri()
            );

            trimmer.trimBroadcasts(item.getPublisher(), broadcastInterval, channel, acceptableIds);

            scheduleWriter.replaceScheduleBlock(
                    item.getPublisher(),
                    channel,
                    ImmutableSet.of(new ScheduleEntry.ItemRefAndBroadcast(item, broadcast))
            );
        }
    }

    /**
     * @param publishers the set of publishers whose explicit equivalences will be modified in the lookup table,
     *                   with any other explicit equivalences to content from other publishers being unaffected.
     *                   If null will be later set to all publishers present in the explicitEquivRefs.
     */
    public void updateExplicitEquivalence(
            Content content,
            @Nullable Set<Publisher> publishers,
            Set<LookupRef> explicitEquivRefs
    ) {
        content.setEquivalentTo(explicitEquivRefs);
        if(content instanceof  Item) {
            writer.createOrUpdate((Item) content, publishers, true);
        } else {
            writer.createOrUpdate((Container) content, publishers, true);
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
