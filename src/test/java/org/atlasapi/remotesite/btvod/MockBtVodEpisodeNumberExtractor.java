package org.atlasapi.remotesite.btvod;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

public class MockBtVodEpisodeNumberExtractor implements BtVodEpisodeNumberExtractor {

    @Override
    public Integer extractEpisodeNumber(BtVodEntry row) {
        String episodeNumber = Iterables.getOnlyElement(
                row.getProductScopes()
        ).getProductMetadata().getEpisodeNumber();
        if (episodeNumber == null) {
            return null;
        }
        return Ints.tryParse(episodeNumber);
    }
}
