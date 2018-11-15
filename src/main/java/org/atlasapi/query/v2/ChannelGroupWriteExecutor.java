package org.atlasapi.query.v2;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.output.AtlasErrorSummary;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupWriteExecutor {

    private static final Logger log = LoggerFactory.getLogger(ChannelGroupWriteExecutor.class);
    private static final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    private static final SubstitutionTableNumberCodec deerCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ChannelGroupStore store;

    private ChannelGroupWriteExecutor(ChannelGroupStore store) {
        this.store = checkNotNull(store);
    }

    public static ChannelGroupWriteExecutor create(ChannelGroupStore store) {
        return new ChannelGroupWriteExecutor(store);
    }

    public Optional<AtlasErrorSummary> createOrUpdateChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response,
            ChannelGroup complexChannelGroup,
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        try {
            if (complexChannelGroup.getId() != null) {
                store.createOrUpdate(complexChannelGroup);
            } else {
                long channelGroupId = store.createOrUpdate(complexChannelGroup).getId();
                com.google.common.base.Optional<ChannelGroup> channelGroupToUpdate = store.channelGroupFor(
                        channelGroupId
                );
                if (channelGroupToUpdate.isPresent()) {
                    ChannelGroup newChannelGroup = channelGroupToUpdate.get();
                    // we update the canonical URI of the new channel group in order to differentiate
                    // it from the previously created ones
                    newChannelGroup.setCanonicalUri(String.format(
                            "http://%s/metabroadcast/%s",
                            newChannelGroup.getPublisher().key(),
                            deerCodec.encode(BigInteger.valueOf(newChannelGroup.getId()))
                    ));
                    // we set the channel group numberings after we create the channel group
                    // because we need to refer the channel group ID in each numbering object
                    updateChannelGroupNumberings(
                            newChannelGroup,
                            simpleChannelGroup
                    );
                    store.createOrUpdate(newChannelGroup);
                } else {
                    log.error("Couldn't find a platform for requested ID {}", channelGroupId);
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    return Optional.of(AtlasErrorSummary.forException(new IllegalArgumentException()));
                }
            }
        } catch (Exception e) {
            log.error(
                    String.format(
                            "Error while creating/updating platform for request %s",
                            request.getRequestURL()
                    ),
                    e
            );
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return Optional.of(AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return Optional.empty();
    }

    public Optional<AtlasErrorSummary> deletePlatform(
            HttpServletRequest request,
            HttpServletResponse response,
            long channelGroupId
    ) {
        try {
            store.deleteChannelGroupById(channelGroupId);
        } catch (Exception e) {
            log.error(
                    String.format(
                            "Error while deleting platform for request %s",
                            request.getRequestURL()
                    ),
                    e
            );
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return Optional.of(AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return Optional.empty();
    }

    private void updateChannelGroupNumberings(
            ChannelGroup channelGroupToBeUpdated,
            org.atlasapi.media.entity.simple.ChannelGroup simple
    ) {
        List<ChannelNumbering> channelNumberingList = Lists.newArrayList();

        simple.getChannels().forEach(channelNumbering -> channelNumberingList.add(
                ChannelNumbering.builder()
                        .withChannel(idCodec.decode(channelNumbering.getChannel().getId()).longValue())
                        .withChannelGroup(channelGroupToBeUpdated.getId())
                        .build()
        ));
        channelGroupToBeUpdated.setChannelNumberings(channelNumberingList);
    }

}
