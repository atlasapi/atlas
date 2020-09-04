package org.atlasapi.query.v2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import joptsimple.internal.Strings;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.output.AtlasErrorSummary;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupWriteExecutor {

    private static final Logger log = LoggerFactory.getLogger(ChannelGroupWriteExecutor.class);
    private static final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    private static final SubstitutionTableNumberCodec deerCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ChannelGroupStore channelGroupStore;
    private final ChannelStore channelStore;

    private ChannelGroupWriteExecutor(
            ChannelGroupStore channelGroupStore,
            ChannelStore channelStore
    ) {
        this.channelGroupStore = checkNotNull(channelGroupStore);
        this.channelStore = checkNotNull(channelStore);
    }

    public static ChannelGroupWriteExecutor create(
            ChannelGroupStore channelGroupStore,
            ChannelStore channelStore
    ) {
        return new ChannelGroupWriteExecutor(channelGroupStore, channelStore);
    }

    public com.google.common.base.Optional<ChannelGroup> createOrUpdateChannelGroup(
            HttpServletRequest request,
            ChannelGroup complex,
            org.atlasapi.media.entity.simple.ChannelGroup simple,
            ChannelResolver channelResolver
    ) {
        try {
            if (complex.getId() != null) {
                return updateChannelGroup(complex, simple.getChannels(), channelResolver);
            }
            if (complex.getCanonicalUri() != null) {
                com.google.common.base.Optional<ChannelGroup> existingChannelGroup = channelGroupStore
                        .channelGroupFor(complex.getCanonicalUri());

                if(existingChannelGroup.isPresent()) {
                    complex.setId(existingChannelGroup.get().getId());
                }
                return updateChannelGroup(complex, simple.getChannels(), channelResolver);
            }
            //if it's a new group, create it
            ChannelGroup newChannelGroup = channelGroupStore.createOrUpdate(complex);

            if(Strings.isNullOrEmpty(newChannelGroup.getCanonicalUri())) {
                //we create a URI that allows us to detect BT has created that group through us
                //and because we use the new ID format for it (all lowercase), we need to create it
                //after it was written to the database.
                //NB: this looks stupid and should be removed - the BT Channel Group Tool UI should
                //    elect the URI
                newChannelGroup.setCanonicalUri(String.format(
                        "http://%s/metabroadcast/%s",
                        newChannelGroup.getPublisher().key(),
                        deerCodec.encode(BigInteger.valueOf(newChannelGroup.getId()))
                ));
                newChannelGroup = channelGroupStore.createOrUpdate(newChannelGroup);
            }
            Set<ChannelNumbering> channelNumberings = simple.getChannels().stream()
                    .map(numbering -> transformChannelNumbering(numbering, complex.getId()))
                    .collect(MoreCollectors.toImmutableSet());
            Set<Long> channelsToUpdate = channelNumberings.stream()
                    .map(ChannelNumbering::getChannel)
                    .collect(MoreCollectors.toImmutableSet());
            updateChannelGroupNumberings(newChannelGroup.getId(), channelNumberings, channelsToUpdate, channelResolver);

            return com.google.common.base.Optional.of(newChannelGroup);
        } catch (Exception e) {
            log.error(
                    "Error while creating/updating platform for request {}",
                    request.getRequestURL(),
                    e
            );
            return com.google.common.base.Optional.absent();
        }
    }

    private ChannelNumbering transformChannelNumbering(
            org.atlasapi.media.entity.simple.ChannelNumbering numbering,
            long channelGroupId
    ) {
        long channelId = decodeOwlChannelId(numbering.getChannel().getId());
        LocalDate startDate = Objects.isNull(numbering.getStartDate())
                ? null
                : LocalDate.fromDateFields(numbering.getStartDate());
        LocalDate endDate = Objects.isNull(numbering.getEndDate())
                ? null
                : LocalDate.fromDateFields(numbering.getEndDate());
        return ChannelNumbering.builder()
                .withChannel(channelId)
                .withChannelGroup(channelGroupId)
                .withChannelNumber(numbering.getChannelNumber())
                .withStartDate(startDate)
                .withEndDate(endDate)
                .build();
    }

    private com.google.common.base.Optional<ChannelGroup> updateChannelGroup(
            ChannelGroup complex,
            List<org.atlasapi.media.entity.simple.ChannelNumbering> simpleNumberings,
            ChannelResolver channelResolver
    ) {
        // These aren't always set by the complexifier because sometimes only a uri is initially specified on the channel
        // group, with the channel numberings requiring a channel group id
        if (complex.getChannelNumberings().isEmpty() && !simpleNumberings.isEmpty()) {
            complex.setChannelNumberings(
                    simpleNumberings.stream()
                            .map(numbering -> transformChannelNumbering(numbering, complex.getId()))
                            .collect(MoreCollectors.toImmutableSet())
            );
        }
        Set<ChannelNumbering> existingChannelNumberings = getExistingChannelNumberings(complex.getId());
        ChannelGroup channelGroup = channelGroupStore.createOrUpdate(complex);
        updateChannelGroupNumberings(
                complex,
                existingChannelNumberings,
                channelResolver
        );
        return com.google.common.base.Optional.of(channelGroup);
    }

    private Set<ChannelNumbering> getExistingChannelNumberings(Long channelGroupId) {
        com.google.common.base.Optional<ChannelGroup> channelGroupOptional = channelGroupStore.channelGroupFor(channelGroupId);
        if (!channelGroupOptional.isPresent()) {
            log.info("Couldn't find channel group for ID {}", channelGroupId);
            return Sets.newHashSet();
        }
        return channelGroupOptional.get().getChannelNumberings();
    }

    private void updateChannelGroupNumberings(
            ChannelGroup complex,
            Set<ChannelNumbering> existingChannelNumberings,
            ChannelResolver channelResolver
    ) {
        //Channel ids corresponding to any channel numberings between the two sets which have changes
        Set<Long> channelsToUpdate = Sets.union(
                Sets.difference(complex.getChannelNumberings(), existingChannelNumberings),
                Sets.difference(existingChannelNumberings, complex.getChannelNumberings())

        ).stream()
                .map(ChannelNumbering::getChannel)
                .collect(MoreCollectors.toImmutableSet());

        updateChannelGroupNumberings(complex.getId(), complex.getChannelNumberings(), channelsToUpdate, channelResolver);
    }

    private void updateChannelGroupNumberings(
            long channelGroupId,
            Set<ChannelNumbering> newNumberings,
            Set<Long> channelsToUpdate,
            ChannelResolver channelResolver
    ) {
        SetMultimap<Long, ChannelNumbering> channelNumberingsByChannel = HashMultimap.create();

        for (ChannelNumbering numbering : newNumberings) {
            channelNumberingsByChannel.put(numbering.getChannel(), numbering);
        }

        for (Long newChannelId : channelsToUpdate) {
            Set<ChannelNumbering> channelNumberingsForChannel = channelNumberingsByChannel.get(newChannelId);
            channelResolver.refreshCache();
            Maybe<Channel> resolvedChannel = channelResolver.fromId(newChannelId);
            if (!resolvedChannel.hasValue()) {
                log.warn("Couldn't resolve channel for ID {}", newChannelId);
                return;
            }
            Channel channel = resolvedChannel.requireValue();

            Set<ChannelNumbering> channelNumberingsToKeep = channel.getChannelNumbers().stream()
                    .filter(numbering -> !numbering.getChannelGroup().equals(channelGroupId))
                    .collect(Collectors.toSet());

            channel.setChannelNumbers(Sets.union(channelNumberingsToKeep, channelNumberingsForChannel));

            // updating the channel also updates the channel numbering on the channel group
            channelStore.createOrUpdate(channel);
        }
    }

    private long decodeOwlChannelId(String channelId) {
        return idCodec.decode(channelId).longValue();
    }

    public Optional<AtlasErrorSummary> deletePlatform(
            HttpServletRequest request,
            HttpServletResponse response,
            long channelGroupId,
            ChannelResolver channelResolver
    ) {
        try {
            Set<ChannelNumbering> channelNumberings = getExistingChannelNumberings(channelGroupId);
            Set<Long> channelIds = channelNumberings.stream()
                    .map(ChannelNumbering::getChannel)
                    .collect(MoreCollectors.toImmutableSet());
            updateChannelGroupNumberings(
                    channelGroupId,
                    ImmutableSet.of(),
                    channelIds,
                    channelResolver
            );
            channelGroupStore.deleteChannelGroupById(channelGroupId);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error while deleting platform for request %s",
                    request.getRequestURL()
            );
            log.error(errorMessage, e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return Optional.of(AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return Optional.empty();
    }

}
