package org.atlasapi.equiv.channel;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ForcedEquivChannelMatcher;
import org.atlasapi.equiv.channel.updaters.BarbForcedChannelEquivalenceUpdater;
import org.atlasapi.equiv.channel.updaters.MultipleSourceChannelEquivalenceUpdater;
import org.atlasapi.equiv.channel.updaters.SourceSpecificChannelEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("PublicConstructor")
@Configuration
public class ChannelEquivModule {

    private static final Logger log = LoggerFactory.getLogger(ChannelEquivModule.class);
    private static final String EQUIV_MAPPING_PATH = "equiv/equivList.csv";

    private final Map<Publisher, ChannelMatcher> channelMatchers = ImmutableMap.of(
            Publisher.BT_TV_CHANNELS, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS),
            Publisher.BT_TV_CHANNELS_TEST1, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_TEST1),
            Publisher.BT_TV_CHANNELS_TEST2, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_TEST2),
            Publisher.BT_TV_CHANNELS_REFERENCE, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_REFERENCE)
            // Leave out Publisher.BARB_CHANNELS as it has a custom equiv updater so we don't want it to be auto built
    );

    @Autowired private ChannelWriter channelWriter;
    @Autowired private ChannelResolver channelResolver;

    private final Map<String, String> barbStationCodeToUriEquivMap;

    public ChannelEquivModule() {
        this.barbStationCodeToUriEquivMap = checkNotNull(parseFile());
    }

    @Bean
    public EquivalenceUpdater<Channel> channelUpdater() {
        MultipleSourceChannelEquivalenceUpdater updaters = MultipleSourceChannelEquivalenceUpdater.create();

        registerPublisherUpdaters(
                updaters,
                channelMatchers.keySet()
        );

        return updaters;
    }

    private void registerPublisherUpdaters(
            MultipleSourceChannelEquivalenceUpdater updaters,
            Set<Publisher> publishers
    ) {
        publishers.forEach(
                publisher -> updaters.register(publisher, createUpdaterFor(publisher))
        );

        updaters.register(Publisher.BARB_CHANNELS, createBarbForceEquivUpdater());
    }

    private EquivalenceUpdater<Channel> createUpdaterFor(Publisher publisher) {
        return SourceSpecificChannelEquivalenceUpdater.builder()
                .forPublisher(publisher)
                .withChannelMatcher(channelMatchers.get(publisher))
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .withMetadata(
                        ChannelEquivalenceUpdaterMetadata.create(channelMatchers.get(publisher), publisher)
                )
                .build();
    }

    private EquivalenceUpdater<Channel> createBarbForceEquivUpdater() {
        ChannelMatcher forcedBarbMatcher = ForcedEquivChannelMatcher.create(Publisher.BARB_CHANNELS);

        return BarbForcedChannelEquivalenceUpdater.create(
                SourceSpecificChannelEquivalenceUpdater.builder()
                    .forPublisher(Publisher.BARB_CHANNELS)
                    .withChannelMatcher(forcedBarbMatcher)
                    .withChannelResolver(channelResolver)
                    .withChannelWriter(channelWriter)
                    .withMetadata(ChannelEquivalenceUpdaterMetadata.create(forcedBarbMatcher, Publisher.BARB_CHANNELS)),
                channelResolver,
                channelWriter,
                barbStationCodeToUriEquivMap
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseFile() {
        try{
            List<String> lines = FileUtils.readLines(new File(EQUIV_MAPPING_PATH));
            Map<String, String> codeMap = Maps.newHashMap();

            // add to map even if no mapping. Need to know for removing equiv later
            lines.forEach(line -> {
                String[] splitLine = line.split(",");
                codeMap.put(splitLine[0], splitLine.length > 1 ? splitLine[1] : null);
            });

            return ImmutableMap.copyOf(codeMap);
        } catch (IOException e) {
            log.error("Failed to parse barb equiv tsv at path {}", EQUIV_MAPPING_PATH, e);
            return null;
        }
    }
}
