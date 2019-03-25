package org.atlasapi.equiv.channel.updaters;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.columbus.telescope.client.EntityType;
import joptsimple.internal.Strings;
import org.atlasapi.equiv.ChannelRef;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;


public class BarbForcedChannelEquivalenceUpdater extends SourceSpecificChannelEquivalenceUpdater {

    private static final Logger log = LoggerFactory.getLogger(BarbForcedChannelEquivalenceUpdater.class);

    private static final String ALIAS_NAMESPACE = "gb:barb:stationCode";

    private final Map<String, String> stationCodeToUri;
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;

    private BarbForcedChannelEquivalenceUpdater(
            SourceSpecificChannelEquivalenceUpdater.Builder delegate,
            ChannelResolver channelResolver,
            ChannelWriter channelWriter,
            Map<String, String> stationCodeToUri
    ) {
        super(delegate);
        this.channelResolver = checkNotNull(channelResolver);
        this.channelWriter = checkNotNull(channelWriter);
        this.stationCodeToUri = checkNotNull(stationCodeToUri);
    }

    public static BarbForcedChannelEquivalenceUpdater create(
            SourceSpecificChannelEquivalenceUpdater.Builder delegate,
            ChannelResolver channelResolver,
            ChannelWriter channelWriter,
            Map<String, String> stationCodeToUri
    ) {
        return new BarbForcedChannelEquivalenceUpdater(
                delegate, channelResolver, channelWriter, stationCodeToUri);
    }

    @Override
    public boolean updateEquivalences(Channel subject, OwlTelescopeReporter telescope) {

        Optional<String> subjectStationCode = getStationCodeFromChannel(subject);

        if (!subjectStationCode.isPresent()) {
            // every barb sourced channel should be in this map
            log.error("No station code found for channel {}", subject.getId());
            return false;
        }

        String candidateUri = stationCodeToUri.get(subjectStationCode.get());

        if (Strings.isNullOrEmpty(candidateUri)) {
            // some things we don't force any equiv on
            log.info("No hardcoded equiv found for channel {}. Removing if present.", subject.getId());
            // check if we need to remove an equiv link though
            removeEquivalence(subject, telescope);
            return true;
        }

        Optional<Channel> optionalCandidate = channelResolver.fromUri(candidateUri).toOptional();

        if (!optionalCandidate.isPresent()) {
            // candidates should always exist if there's a url for them
            log.error("No channel found for uri {}", candidateUri);
            return false;
        }

        Channel candidate = optionalCandidate.get();

        // if subject is already equiv to candidate, don't do anything
        if (!subject.getSameAs().contains(candidate.toChannelRef())) {
            // if its not empty, it already has an equiv which needs removing as its been reassigned
            if (!subject.getSameAs().isEmpty()) {
                removeEquivalence(subject, telescope);
            }
            setAndUpdateEquivalents(candidate, subject, telescope);
        }
        return true;
    }


    private Optional<String> getStationCodeFromChannel(Channel channel) {
        return channel.getAliases().stream()
                .filter(a -> a.getNamespace().equals(ALIAS_NAMESPACE))
                .findFirst()
                .map(Alias::getValue);
    }

    private void removeEquivalence(Channel subject, OwlTelescopeReporter telescope) {
        if (subject.getSameAs().isEmpty()) {
            return; //no equiv to remove
        }
        
        ChannelRef candidateRef = subject.getSameAs().iterator().next();
        Channel candidate = channelResolver.fromUri(candidateRef.getUri()).requireValue();

        Set<ChannelRef> candidateRefs = candidate.getSameAs();
        if (candidateRefs.contains(subject.toChannelRef())) {
            subject.setSameAs(ImmutableSet.of());
            candidate.setSameAs(
                    candidateRefs.stream()
                            .filter(ref -> ref.getId() != subject.getId())
                            .collect(Collectors.toSet())
            );

            channelWriter.createOrUpdate(candidate);
            channelWriter.createOrUpdate(subject);

            telescope.reportSuccessfulEvent(
                    subject.getId(),
                    subject.getAliases(),
                    EntityType.CHANNEL,
                    subject
            );

            telescope.reportSuccessfulEvent(
                    candidate.getId(),
                    candidate.getAliases(),
                    EntityType.CHANNEL,
                    candidate
            );

            log.info("Equiv link removed for {} to {}", subject.getId(), candidate.getId());
        }

    }
}
