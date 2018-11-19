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

    public Optional<ChannelGroup> createOrUpdateChannelGroup(
            HttpServletRequest request,
            ChannelGroup complexChannelGroup,
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        try {

            if (complexChannelGroup.getId() == null) {
                //store the new group in the database
                complexChannelGroup = store.createOrUpdate(complexChannelGroup);

                // we create a URI that allows us to detect BT has created that group through us
                //and because we use the new ID format for it (all lowercase), we need to create it
                //after it was written to the database.
                complexChannelGroup.setCanonicalUri(String.format(
                        "http://%s/metabroadcast/%s",
                        complexChannelGroup.getPublisher().key(),
                    deerCodec.encode(BigInteger.valueOf(complexChannelGroup.getId()))
                ));
            }

            // in either case update the channel list.
            // we set the channel group numberings after we create the channel group
            // because we need to refer the channel group ID in each numbering object
            updateChannelGroupNumberings(
                    complexChannelGroup,
                    simpleChannelGroup
            );

            return Optional.ofNullable(store.createOrUpdate(complexChannelGroup));
        } catch (Exception e) {
            log.error("Error while creating/updating platform for request {}",
                            request.getRequestURL(), e);
            return Optional.empty();
        }
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
