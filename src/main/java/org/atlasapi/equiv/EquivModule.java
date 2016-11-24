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

import java.io.File;

import org.atlasapi.equiv.results.persistence.FileEquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.MultipleSourceEquivalenceUpdater;
import org.atlasapi.equiv.update.SourceSpecificEquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.configuration.UpdaterConfiguration;
import org.atlasapi.equiv.update.updaters.configuration.UpdaterConfigurationRegistry;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.MessageSender;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({ KafkaMessagingModule.class })
public class EquivModule {

    private @Value("${equiv.results.directory}") String equivResultsDirectory;
    private @Value("${messaging.destination.equiv.assert}") String equivAssertDest;
    private @Value("${equiv.excludedUris}") String excludedUris;

    private @Autowired ScheduleResolver scheduleResolver;
    private @Autowired SearchResolver searchResolver;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired EquivalenceSummaryStore equivSummaryStore;
    private @Autowired LookupWriter lookupWriter;
    private @Autowired LookupEntryStore lookupEntryStore;

    private @Autowired KafkaMessagingModule messaging;

    @Bean
    public EquivalenceUpdater<Content> contentUpdater() {
        EquivalenceUpdaterProviderDependencies dependencies = EquivalenceUpdaterProviderDependencies
                .builder()
                .withScheduleResolver(scheduleResolver)
                .withSearchResolver(searchResolver)
                .withContentResolver(contentResolver)
                .withChannelResolver(channelResolver)
                .withEquivSummaryStore(equivSummaryStore)
                .withLookupWriter(lookupWriter)
                .withLookupEntryStore(lookupEntryStore)
                .withEquivalenceResultStore(equivalenceResultStore())
                .withMessageSender(equivAssertDestination())
                .withExcludedUris(excludedUrisFromProperties())
                .build();

        UpdaterConfigurationRegistry registry = UpdaterConfigurationRegistry.create();

        MultipleSourceEquivalenceUpdater updaters = MultipleSourceEquivalenceUpdater.create();

        for (UpdaterConfiguration configuration : registry.getUpdaterConfigurations()) {
            EquivalenceUpdater<Item> itemEquivalenceUpdater = configuration
                    .getItemEquivalenceUpdaterType()
                    .getProvider()
                    .getUpdater(
                            dependencies,
                            configuration.getItemEquivalenceTargetSources()
                    );

            EquivalenceUpdater<Container> topLevelContainerEquivalenceUpdater = configuration
                    .getTopLevelContainerEquivalenceUpdaterType()
                    .getProvider()
                    .getUpdater(
                            dependencies,
                            configuration.getTopLevelContainerTargetSources()
                    );

            EquivalenceUpdater<Container> nonTopLevelContainerEquivalenceUpdater = configuration
                    .getNonTopLevelContainerEquivalenceUpdaterType()
                    .getProvider()
                    .getUpdater(
                            dependencies,
                            configuration.getNonTopLevelContainerTargetSources()
                    );

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

    private ImmutableSet<String> excludedUrisFromProperties() {
        if (Strings.isNullOrEmpty(excludedUris)) {
            return ImmutableSet.of();
        }
        return ImmutableSet.copyOf(Splitter.on(',').split(excludedUris));
    }
}
