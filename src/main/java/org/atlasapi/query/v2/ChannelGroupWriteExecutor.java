package org.atlasapi.query.v2;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.output.AtlasErrorSummary;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import joptsimple.internal.Strings;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                Set<Long> existingChannelIds = getChannelIdsForChannelGroupId(complex.getId());
                ChannelGroup channelGroup = channelGroupStore.createOrUpdate(complex);
                updateChannelGroupNumberings(
                        complex,
                        simple.getChannels(),
                        existingChannelIds,
                        channelResolver
                );
                return com.google.common.base.Optional.of(channelGroup);
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
            addNewNumberingsToChannels(newChannelGroup, simple.getChannels(), channelResolver);

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

    private Set<Long> getChannelIdsForChannelGroupId(Long channelGroupId) {
        com.google.common.base.Optional<ChannelGroup> channelGroupOptional = channelGroupStore.channelGroupFor(channelGroupId);
        if (!channelGroupOptional.isPresent()) {
            log.info("Couldn't find channel group for ID {}", channelGroupId);
            return Sets.newHashSet();
        }

        return channelGroupOptional.get()
                .getChannelNumberings()
                .stream()
                .map(ChannelNumbering::getChannel)
                .collect(Collectors.toSet());
    }

    private void updateChannelGroupNumberings(
            ChannelGroup complex,
            List<org.atlasapi.media.entity.simple.ChannelNumbering> channelNumberings,
            Set<Long> existingChannelIds,
            ChannelResolver channelResolver
    ) {
        Set<Long> channelIds = channelNumberings.stream()
                .map(channelNumbering -> decodeOwlChannelId(channelNumbering.getChannel().getId()))
                .collect(Collectors.toSet());

        SetView<Long> staleChannelIds = Sets.difference(existingChannelIds, channelIds);
        removeStaleChannelNumbersForChannels(
                complex.getId(),
                staleChannelIds,
                channelResolver
        );

        List<org.atlasapi.media.entity.simple.ChannelNumbering> newChannelNumberings = channelNumberings.stream()
                .filter(channelNumbering -> !exist(existingChannelIds, channelNumbering))
                .collect(Collectors.toList());
        addNewNumberingsToChannels(complex, newChannelNumberings, channelResolver);
    }

    private boolean exist(
            Set<Long> existingChannelIds,
            org.atlasapi.media.entity.simple.ChannelNumbering channelNumbering
    ) {
        return existingChannelIds.contains(
                decodeOwlChannelId(channelNumbering.getChannel().getId())
        );
    }

    private void addNewNumberingsToChannels(
            ChannelGroup channelGroup,
            List<org.atlasapi.media.entity.simple.ChannelNumbering> newChannelNumberings,
            ChannelResolver channelResolver
    ) {
        newChannelNumberings.forEach(numbering -> {
            long newChannelId = decodeOwlChannelId(numbering.getChannel().getId());
            channelResolver.refreshCache();
            Maybe<Channel> resolvedChannel = channelResolver.fromId(newChannelId);
            if (!resolvedChannel.hasValue()) {
                log.warn("Couldn't resolve channel for ID {}", newChannelId);
                return;
            }
            Channel channel = resolvedChannel.requireValue();

            if (channelContainsNumbering(channel, newChannelId, channelGroup.getId())) {
                log.warn(
                        "New channel numbering with channel group {} and channel {} already exist for channel {}",
                        channelGroup.getId(),
                        newChannelId,
                        channel.getId()
                );
                return;
            }

            LocalDate startDate = Objects.isNull(numbering.getStartDate())
                                  ? null
                                  : LocalDate.fromDateFields(numbering.getStartDate());
            LocalDate endDate = Objects.isNull(numbering.getEndDate())
                                ? null
                                : LocalDate.fromDateFields(numbering.getEndDate());
            channel.addChannelNumber(
                    channelGroup,
                    numbering.getChannelNumber(),
                    startDate,
                    endDate
            );

            // updating the channel also updates the channel numbering on the channel group
            channelStore.createOrUpdate(channel);
        });
    }

    private boolean channelContainsNumbering(
            Channel channel,
            long newChannelId,
            long channelGroupId
    ) {
        return channel.getChannelNumbers()
                .stream()
                .anyMatch(channelNumbering -> channelNumbering.getChannelGroup() == channelGroupId
                        && channelNumbering.getChannel() == newChannelId
                );
    }

    private long decodeOwlChannelId(String channelId) {
        return idCodec.decode(channelId).longValue();
    }

    private void removeStaleChannelNumbersForChannels(
            Long channelGroupId,
            Set<Long> staleChannelIds,
            ChannelResolver channelResolver
    ) {
        staleChannelIds.forEach(channelId -> {
            channelResolver.refreshCache();
            Maybe<Channel> resolvedChannel = channelResolver.fromId(channelId);
            if (!resolvedChannel.hasValue()) {
                log.warn("Couldn't resolve channel for ID {}", channelId);
                return;
            }
            Channel channel = resolvedChannel.requireValue();

            Set<ChannelNumbering> updatedChannelNumberings = channel.getChannelNumbers()
                    .stream()
                    .filter(channelNumbering -> !isLinkedToChannelGroup(
                            channelGroupId,
                            channelNumbering
                    ))
                    .collect(Collectors.toSet());
            channel.setChannelNumbers(updatedChannelNumberings);

            channelStore.createOrUpdate(channel);
        });
    }

    private boolean isLinkedToChannelGroup(
            Long channelGroupId,
            ChannelNumbering channelNumbering
    ) {
        return channelNumbering.getChannelGroup().equals(channelGroupId);
    }

    public Optional<AtlasErrorSummary> deletePlatform(
            HttpServletRequest request,
            HttpServletResponse response,
            long channelGroupId,
            ChannelResolver channelResolver
    ) {
        try {
            Set<Long> channelIds = getChannelIdsForChannelGroupId(channelGroupId);
            removeStaleChannelNumbersForChannels(
                    channelGroupId,
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
