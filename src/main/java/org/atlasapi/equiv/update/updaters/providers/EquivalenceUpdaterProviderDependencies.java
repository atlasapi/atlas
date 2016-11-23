package org.atlasapi.equiv.update.updaters.providers;

import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.MessageSender;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceUpdaterProviderDependencies {

    private final ScheduleResolver scheduleResolver;
    private final SearchResolver searchResolver;
    private final ContentResolver contentResolver;
    private final ChannelResolver channelResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final LookupWriter lookupWriter;
    private final LookupEntryStore lookupEntryStore;
    private final RecentEquivalenceResultStore equivalenceResultStore;
    private final MessageSender<ContentEquivalenceAssertionMessage> messageSender;
    private final ImmutableSet<String> excludedUris;

    private EquivalenceUpdaterProviderDependencies(
            ScheduleResolver scheduleResolver,
            SearchResolver searchResolver,
            ContentResolver contentResolver,
            ChannelResolver channelResolver,
            EquivalenceSummaryStore equivSummaryStore,
            LookupWriter lookupWriter,
            LookupEntryStore lookupEntryStore,
            RecentEquivalenceResultStore equivalenceResultStore,
            MessageSender<ContentEquivalenceAssertionMessage> messageSender,
            ImmutableSet<String> excludedUris
    ) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.searchResolver = checkNotNull(searchResolver);
        this.contentResolver = checkNotNull(contentResolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.equivSummaryStore = checkNotNull(equivSummaryStore);
        this.lookupWriter = checkNotNull(lookupWriter);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.equivalenceResultStore = checkNotNull(equivalenceResultStore);
        this.messageSender = checkNotNull(messageSender);
        this.excludedUris = ImmutableSet.copyOf(excludedUris);
    }

    public static ScheduleResolverStep builder() {
        return new Builder();
    }

    public ScheduleResolver getScheduleResolver() {
        return scheduleResolver;
    }

    public SearchResolver getSearchResolver() {
        return searchResolver;
    }

    public ContentResolver getContentResolver() {
        return contentResolver;
    }

    public ChannelResolver getChannelResolver() {
        return channelResolver;
    }

    public EquivalenceSummaryStore getEquivSummaryStore() {
        return equivSummaryStore;
    }

    public LookupWriter getLookupWriter() {
        return lookupWriter;
    }

    public LookupEntryStore getLookupEntryStore() {
        return lookupEntryStore;
    }

    public RecentEquivalenceResultStore getEquivalenceResultStore() {
        return equivalenceResultStore;
    }

    public MessageSender<ContentEquivalenceAssertionMessage> getMessageSender() {
        return messageSender;
    }

    public ImmutableSet<String> getExcludedUris() {
        return excludedUris;
    }

    public interface ScheduleResolverStep {

        SearchResolverStep withScheduleResolver(ScheduleResolver scheduleResolver);
    }

    public interface SearchResolverStep {

        ContentResolverStep withSearchResolver(SearchResolver searchResolver);
    }

    public interface ContentResolverStep {

        ChannelResolverStep withContentResolver(ContentResolver contentResolver);
    }

    public interface ChannelResolverStep {

        EquivSummaryStoreStep withChannelResolver(ChannelResolver channelResolver);
    }

    public interface EquivSummaryStoreStep {

        LookupWriterStep withEquivSummaryStore(EquivalenceSummaryStore equivSummaryStore);
    }

    public interface LookupWriterStep {

        LookupEntryStoreStep withLookupWriter(LookupWriter lookupWriter);
    }

    public interface LookupEntryStoreStep {

        EquivalenceResultStoreStep withLookupEntryStore(LookupEntryStore lookupEntryStore);
    }

    public interface EquivalenceResultStoreStep {

        MessageSenderStep withEquivalenceResultStore(
                RecentEquivalenceResultStore equivalenceResultStore);
    }

    public interface MessageSenderStep {

        ExcludedUrisStep withMessageSender(
                MessageSender<ContentEquivalenceAssertionMessage> messageSender);
    }

    public interface ExcludedUrisStep {

        BuildStep withExcludedUris(ImmutableSet<String> excludedUris);
    }

    public interface BuildStep {

        EquivalenceUpdaterProviderDependencies build();
    }

    public static class Builder
            implements ScheduleResolverStep, SearchResolverStep, ContentResolverStep,
            ChannelResolverStep, EquivSummaryStoreStep, LookupWriterStep, LookupEntryStoreStep,
            EquivalenceResultStoreStep, MessageSenderStep, ExcludedUrisStep, BuildStep {

        private ScheduleResolver scheduleResolver;
        private SearchResolver searchResolver;
        private ContentResolver contentResolver;
        private ChannelResolver channelResolver;
        private EquivalenceSummaryStore equivSummaryStore;
        private LookupWriter lookupWriter;
        private LookupEntryStore lookupEntryStore;
        private RecentEquivalenceResultStore equivalenceResultStore;
        private MessageSender<ContentEquivalenceAssertionMessage> messageSender;
        private ImmutableSet<String> excludedUris;

        private Builder() {
        }

        @Override
        public SearchResolverStep withScheduleResolver(ScheduleResolver scheduleResolver) {
            this.scheduleResolver = scheduleResolver;
            return this;
        }

        @Override
        public ContentResolverStep withSearchResolver(SearchResolver searchResolver) {
            this.searchResolver = searchResolver;
            return this;
        }

        @Override
        public ChannelResolverStep withContentResolver(ContentResolver contentResolver) {
            this.contentResolver = contentResolver;
            return this;
        }

        @Override
        public EquivSummaryStoreStep withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        @Override
        public LookupWriterStep withEquivSummaryStore(EquivalenceSummaryStore equivSummaryStore) {
            this.equivSummaryStore = equivSummaryStore;
            return this;
        }

        @Override
        public LookupEntryStoreStep withLookupWriter(LookupWriter lookupWriter) {
            this.lookupWriter = lookupWriter;
            return this;
        }

        @Override
        public EquivalenceResultStoreStep withLookupEntryStore(LookupEntryStore lookupEntryStore) {
            this.lookupEntryStore = lookupEntryStore;
            return this;
        }

        @Override
        public MessageSenderStep withEquivalenceResultStore(
                RecentEquivalenceResultStore equivalenceResultStore) {
            this.equivalenceResultStore = equivalenceResultStore;
            return this;
        }

        @Override
        public ExcludedUrisStep withMessageSender(
                MessageSender<ContentEquivalenceAssertionMessage> messageSender) {
            this.messageSender = messageSender;
            return this;
        }

        @Override
        public BuildStep withExcludedUris(ImmutableSet<String> excludedUris) {
            this.excludedUris = excludedUris;
            return this;
        }

        @Override
        public EquivalenceUpdaterProviderDependencies build() {
            return new EquivalenceUpdaterProviderDependencies(
                    this.scheduleResolver,
                    this.searchResolver,
                    this.contentResolver,
                    this.channelResolver,
                    this.equivSummaryStore,
                    this.lookupWriter,
                    this.lookupEntryStore,
                    this.equivalenceResultStore,
                    this.messageSender,
                    this.excludedUris
            );
        }
    }
}
