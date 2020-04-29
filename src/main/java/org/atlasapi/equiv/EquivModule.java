/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.equiv;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.results.persistence.FileEquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.MultipleSourceEquivalenceUpdater;
import org.atlasapi.equiv.update.SourceSpecificEquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.configuration.UpdaterConfiguration;
import org.atlasapi.equiv.update.updaters.configuration.UpdaterConfigurationRegistry;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType;
import org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.util.Collection;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({ KafkaMessagingModule.class })
public class EquivModule {

    @Value("${equiv.results.directory}") private String equivResultsDirectory;
    @Value("${messaging.destination.equiv.assert}") private String equivAssertDest;
    @Value("${equiv.excludedUris}") private String excludedUris;
    @Value("${equiv.excludedIds}") private String excludedIds;

    @Autowired private ScheduleResolver scheduleResolver;
    @Autowired @Qualifier("EquivalenceSearchResolver") private SearchResolver owlSearchResolver;
    @Autowired @Qualifier("DeerSearchResolver") private SearchResolver deerSearchResolver;
    @Autowired private ContentResolver contentResolver;
    @Autowired private ChannelResolver channelResolver;
    @Autowired private EquivalenceSummaryStore equivSummaryStore;
    @Autowired private LookupWriter lookupWriter;
    @Autowired private LookupEntryStore lookupEntryStore;

    @Autowired private KafkaMessagingModule messaging;

    @Autowired private AmazonTitleIndexStore amazonTitleIndexStore;

    @Bean @Qualifier("contentUpdater")
    public MultipleSourceEquivalenceUpdater contentUpdater() {
        EquivalenceUpdaterProviderDependencies dependencies = EquivalenceUpdaterProviderDependencies
                .builder()
                .withScheduleResolver(scheduleResolver)
                .withOwlSearchResolver(owlSearchResolver)
                .withDeerSearchResolver(deerSearchResolver)
                .withContentResolver(contentResolver)
                .withChannelResolver(channelResolver)
                .withEquivSummaryStore(equivSummaryStore)
                .withLookupWriter(lookupWriter)
                .withLookupEntryStore(lookupEntryStore)
                .withEquivalenceResultStore(equivalenceResultStore())
                .withMessageSender(equivAssertDestination())
                .withExcludedUris(excludedContentFromProperties(excludedUris))
                .withExcludedIds(excludedContentFromProperties(excludedIds))
                .withAmazonTitleIndexStore(amazonTitleIndexStore)
                .build();

        UpdaterConfigurationRegistry registry = UpdaterConfigurationRegistry.create();

        MultipleSourceEquivalenceUpdater updaters = MultipleSourceEquivalenceUpdater.create();

        for (UpdaterConfiguration configuration : registry.getUpdaterConfigurations()) {

            ContentEquivalenceUpdater<Item> itemEquivalenceUpdater =
                    createItemEquivalenceUpdater(configuration, dependencies);

            ContentEquivalenceUpdater<Container> topLevelContainerEquivalenceUpdater =
                    createTopLevelContainerEquivalenceUpdater(configuration, dependencies);

            ContentEquivalenceUpdater<Container> nonTopLevelContainerEquivalenceUpdater =
                    createNonTopLevelContainerEquivalenceUpdater(configuration, dependencies);

            updaters.register(
                    configuration.getSource(),
                    SourceSpecificEquivalenceUpdater
                            .builder(configuration.getSource())
                            .withItemUpdater(itemEquivalenceUpdater)
                            .withTopLevelContainerUpdater(topLevelContainerEquivalenceUpdater)
                            .withNonTopLevelContainerUpdater(nonTopLevelContainerEquivalenceUpdater)
                            .build()
            );
        }

        return updaters;
    }

    private ContentEquivalenceUpdater<Item> createItemEquivalenceUpdater(
            UpdaterConfiguration configuration,
            EquivalenceUpdaterProviderDependencies dependencies
    ) {
        ImmutableSet.Builder<EquivalenceResultUpdater<Item>> itemEquivalenceResultUpdaters
                = ImmutableSet.builder();
        for (ItemEquivalenceUpdaterType updaterType : configuration.getItemEquivalenceUpdaters().keySet()) {
            itemEquivalenceResultUpdaters.add(
                    updaterType.getProvider().getUpdater(
                            dependencies,
                            configuration.getItemEquivalenceUpdaters().get(updaterType)
                    )
            );
        }

        ImmutableSet<Publisher> allItemTargetPublishers = configuration
                .getItemEquivalenceUpdaters()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(MoreCollectors.toImmutableSet());

        EquivalenceResultHandler<Item> itemEquivalenceResultHandler =
                configuration.getItemEquivalenceHandlerType()
                        .getHandlerProvider()
                        .getHandler(dependencies, allItemTargetPublishers);

        EquivalenceResultMessenger<Item> itemEquivalenceResultMessenger =
                configuration.getItemEquivalenceMessengerType()
                        .getMessengerProvider()
                        .getMessenger(dependencies, allItemTargetPublishers);

        return ContentEquivalenceUpdater
                .<Item>builder()
                .withEquivalenceResultUpdaters(itemEquivalenceResultUpdaters.build())
                .withHandler(itemEquivalenceResultHandler)
                .withMessenger(itemEquivalenceResultMessenger)
                .build();
    }

    private ContentEquivalenceUpdater<Container> createTopLevelContainerEquivalenceUpdater(
            UpdaterConfiguration configuration,
            EquivalenceUpdaterProviderDependencies dependencies
    ) {
        ImmutableSet.Builder<EquivalenceResultUpdater<Container>> topLevelEquivalenceResultUpdaters
                = ImmutableSet.builder();
        for (ContainerEquivalenceUpdaterType updaterType : configuration.getTopLevelContainerEquivalenceUpdaters().keySet()) {
            topLevelEquivalenceResultUpdaters.add(
                    updaterType.getProvider().getUpdater(
                            dependencies,
                            configuration.getTopLevelContainerEquivalenceUpdaters().get(updaterType)
                    )
            );
        }

        ImmutableSet<Publisher> allTopLevelContainerTargetPublishers = configuration
                .getTopLevelContainerEquivalenceUpdaters()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(MoreCollectors.toImmutableSet());

        EquivalenceResultHandler<Container> topLevelContainerEquivalenceResultHandler =
                configuration.getTopLevelContainerEquivalenceHandlerType()
                        .getHandlerProvider()
                        .getHandler(dependencies, allTopLevelContainerTargetPublishers);

        EquivalenceResultMessenger<Container> topLevelContainerEquivalenceResultMessenger =
                configuration.getTopLevelContainerEquivalenceMessengerType()
                        .getMessengerProvider()
                        .getMessenger(dependencies, allTopLevelContainerTargetPublishers);

        return ContentEquivalenceUpdater
                .<Container>builder()
                .withEquivalenceResultUpdaters(topLevelEquivalenceResultUpdaters.build())
                .withHandler(topLevelContainerEquivalenceResultHandler)
                .withMessenger(topLevelContainerEquivalenceResultMessenger)
                .build();
    }

    private ContentEquivalenceUpdater<Container> createNonTopLevelContainerEquivalenceUpdater(
            UpdaterConfiguration configuration,
            EquivalenceUpdaterProviderDependencies dependencies
    ) {
        ImmutableSet.Builder<EquivalenceResultUpdater<Container>> nonTopLevelEquivalenceResultUpdaters
                = ImmutableSet.builder();
        for (ContainerEquivalenceUpdaterType updaterType : configuration.getNonTopLevelContainerEquivalenceUpdaters().keySet()) {
            nonTopLevelEquivalenceResultUpdaters.add(
                    updaterType.getProvider().getUpdater(
                            dependencies,
                            configuration.getNonTopLevelContainerEquivalenceUpdaters().get(updaterType)
                    )
            );
        }

        ImmutableSet<Publisher> allNonTopLevelContainerTargetPublishers = configuration
                .getNonTopLevelContainerEquivalenceUpdaters()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(MoreCollectors.toImmutableSet());

        EquivalenceResultHandler<Container> nonTopLevelContainerEquivalenceResultHandler =
                configuration.getNonTopLevelContainerEquivalenceHandlerType()
                        .getHandlerProvider()
                        .getHandler(dependencies, allNonTopLevelContainerTargetPublishers);

        EquivalenceResultMessenger<Container> nonTopLevelContainerEquivalenceResultMessenger =
                configuration.getNonTopLevelContainerEquivalenceMessengerType()
                        .getMessengerProvider()
                        .getMessenger(dependencies, allNonTopLevelContainerTargetPublishers);

        return ContentEquivalenceUpdater
                .<Container>builder()
                .withEquivalenceResultUpdaters(nonTopLevelEquivalenceResultUpdaters.build())
                .withHandler(nonTopLevelContainerEquivalenceResultHandler)
                .withMessenger(nonTopLevelContainerEquivalenceResultMessenger)
                .build();
    }

    @Bean
    public RecentEquivalenceResultStore equivalenceResultStore() {
        return new RecentEquivalenceResultStore(
                new FileEquivalenceResultStore(new File(equivResultsDirectory))
        );
    }

    @Bean
    protected MessageSender<ContentEquivalenceAssertionMessage> equivAssertDestination() {
        return messaging.messageSenderFactory()
                .makeMessageSender(
                        equivAssertDest,
                        JacksonMessageSerializer.forType(ContentEquivalenceAssertionMessage.class)
                );
    }

    private ImmutableSet<String> excludedContentFromProperties(String excludedContent) {
        if (Strings.isNullOrEmpty(excludedContent)) {
            return ImmutableSet.of();
        }
        return ImmutableSet.copyOf(Splitter.on(',').split(excludedContent));
    }

}
