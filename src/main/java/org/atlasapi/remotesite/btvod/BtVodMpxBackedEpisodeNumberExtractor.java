package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodMpxBackedEpisodeNumberExtractor implements BtVodEpisodeNumberExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(BtVodMpxBackedEpisodeNumberExtractor.class);
    private static final Pattern EPISODE_NUMBER_EXTRACTINATORER = Pattern.compile(
        ".*[- ][Ee]([0-9]{1,2}).*"
    );

    private final BtMpxVodClient mpxClient;


    public BtVodMpxBackedEpisodeNumberExtractor(BtMpxVodClient mpxClient) {
        this.mpxClient = checkNotNull(mpxClient);
    }

    @Override
    public Integer extractEpisodeNumber(BtVodEntry row) {
        if (!Strings.isNullOrEmpty(row.getParentGuid())) {
            Optional<BtVodEntry> parent = mpxClient.getItem(row.getParentGuid());
            if (parent.isPresent() && BtVodItemExtractor.COLLECTION_TYPE.equalsIgnoreCase(parent.get().getProductType())) {
                LOG.debug("Row {} is part of a collection, omitting episode number", row.getGuid());
                return null;
            }
        }
        String episodeNumber = Iterables.getOnlyElement(
                row.getProductScopes()
        ).getProductMetadata().getEpisodeNumber();
        if (episodeNumber == null) {
            Matcher matcher = EPISODE_NUMBER_EXTRACTINATORER.matcher(row.getTitle());
            if (matcher.find()) {
                return Ints.tryParse(matcher.group(1));
            }
            return null;
        }
        return Ints.tryParse(episodeNumber);
    }
}
