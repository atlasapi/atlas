package org.atlasapi.equiv.update.updaters.providers;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.MessageSender;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.query.content.search.DeerSearchResolver;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceUpdaterProviderDependencies {

    private final ScheduleResolver scheduleResolver;
    private final SearchResolver owlSearchResolver;
    private final DeerSearchResolver deerSearchResolver;
    private final ContentResolver contentResolver;
    private final ChannelResolver channelResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final LookupWriter lookupWriter;
    private final LookupEntryStore lookupEntryStore;
    private final RecentEquivalenceResultStore equivalenceResultStore;
    private final MessageSender<ContentEquivalenceAssertionMessage> messageSender;
    private final ImmutableSet<String> excludedUris;
    private final ImmutableSet<String> excludedIds;

    private final AmazonTitleIndexStore amazonTitleIndexStore;

    private EquivalenceUpdaterProviderDependencies(
            ScheduleResolver scheduleResolver,
            SearchResolver owlSearchResolver,
            DeerSearchResolver deerSearchResolver,
            ContentResolver contentResolver,
            ChannelResolver channelResolver,
            EquivalenceSummaryStore equivSummaryStore,
            LookupWriter lookupWriter,
            LookupEntryStore lookupEntryStore,
            RecentEquivalenceResultStore equivalenceResultStore,
            MessageSender<ContentEquivalenceAssertionMessage> messageSender,
            ImmutableSet<String> excludedUris,
            ImmutableSet<String> excludedIds,
            AmazonTitleIndexStore amazonTitleIndexStore
    ) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.owlSearchResolver = checkNotNull(owlSearchResolver);
        this.deerSearchResolver = checkNotNull(deerSearchResolver);
        this.contentResolver = checkNotNull(contentResolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.equivSummaryStore = checkNotNull(equivSummaryStore);
        this.lookupWriter = checkNotNull(lookupWriter);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.equivalenceResultStore = checkNotNull(equivalenceResultStore);
        this.messageSender = checkNotNull(messageSender);
        this.excludedUris = ImmutableSet.copyOf(excludedUris);
        this.excludedIds = ImmutableSet.copyOf(excludedIds);
        this.amazonTitleIndexStore = checkNotNull(amazonTitleIndexStore);
    }

    public static ScheduleResolverStep builder() {
        return new Builder();
    }

    public ScheduleResolver getScheduleResolver() {
        return scheduleResolver;
    }

    public SearchResolver getOwlSearchResolver() {
        return owlSearchResolver;
    }

    public DeerSearchResolver getDeerSearchResolver() {
        return deerSearchResolver;
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

    public ImmutableSet<String> getExcludedIds() {
        return excludedIds;
    }

    public AmazonTitleIndexStore getAmazonTitleIndexStore() {
        return amazonTitleIndexStore;
    }

    public interface ScheduleResolverStep {

        OwlSearchResolverStep withScheduleResolver(ScheduleResolver scheduleResolver);
    }

    public interface OwlSearchResolverStep {

        DeerSearchResolverStep withOwlSearchResolver(SearchResolver owlSearchResolver);
    }

    public interface DeerSearchResolverStep {

        ContentResolverStep withDeerSearchResolver(DeerSearchResolver deerSearchResolver);
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

        ExcludedIdsStep withExcludedUris(ImmutableSet<String> excludedUris);
    }

    public interface ExcludedIdsStep {

        AmazonTitleIndexStoreStep withExcludedIds(ImmutableSet<String> excludedIds);
    }

    public interface AmazonTitleIndexStoreStep {

        BuildStep withAmazonTitleIndexStore(AmazonTitleIndexStore amazonTitleIndexStore);

    }

    public interface BuildStep {

        EquivalenceUpdaterProviderDependencies build();
    }

    public static class Builder
            implements ScheduleResolverStep, OwlSearchResolverStep, DeerSearchResolverStep, ContentResolverStep,
            ChannelResolverStep, EquivSummaryStoreStep, LookupWriterStep, LookupEntryStoreStep,
            EquivalenceResultStoreStep, MessageSenderStep, ExcludedUrisStep, ExcludedIdsStep,
            AmazonTitleIndexStoreStep,
            BuildStep {

        private ScheduleResolver scheduleResolver;
        private SearchResolver owlSearchResolver;
        private DeerSearchResolver deerSearchResolver;
        private ContentResolver contentResolver;
        private ChannelResolver channelResolver;
        private EquivalenceSummaryStore equivSummaryStore;
        private LookupWriter lookupWriter;
        private LookupEntryStore lookupEntryStore;
        private RecentEquivalenceResultStore equivalenceResultStore;
        private MessageSender<ContentEquivalenceAssertionMessage> messageSender;
        private ImmutableSet<String> excludedUris;
        private ImmutableSet<String> excludedIds;
        private AmazonTitleIndexStore amazonTitleIndexStore;

        private Builder() {
        }

        @Override
        public OwlSearchResolverStep withScheduleResolver(ScheduleResolver scheduleResolver) {
            this.scheduleResolver = scheduleResolver;
            return this;
        }

        @Override
        public DeerSearchResolverStep withOwlSearchResolver(SearchResolver owlSearchResolver) {
            this.owlSearchResolver = owlSearchResolver;
            return this;
        }

        @Override
        public ContentResolverStep withDeerSearchResolver(
                DeerSearchResolver deerSearchResolver) {
            this.deerSearchResolver = deerSearchResolver;
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
        public ExcludedIdsStep withExcludedUris(ImmutableSet<String> excludedUris) {
            this.excludedUris = excludedUris;
            return this;
        }

        @Override
        public AmazonTitleIndexStoreStep withExcludedIds(ImmutableSet<String> excludedIds) {
            this.excludedIds = excludedIds;
            return this;
        }

        @Override
        public BuildStep withAmazonTitleIndexStore(AmazonTitleIndexStore amazonTitleIndexStore) {
            this.amazonTitleIndexStore = amazonTitleIndexStore;
            return this;
        }

        @Override
        public EquivalenceUpdaterProviderDependencies build() {
            return new EquivalenceUpdaterProviderDependencies(
                    this.scheduleResolver,
                    this.owlSearchResolver,
                    this.deerSearchResolver,
                    this.contentResolver,
                    this.channelResolver,
                    this.equivSummaryStore,
                    this.lookupWriter,
                    this.lookupEntryStore,
                    this.equivalenceResultStore,
                    this.messageSender,
                    this.excludedUris,
                    this.excludedIds,
                    this.amazonTitleIndexStore
            );
        }
    }
}
