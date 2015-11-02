package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

public interface BtVodEpisodeNumberExtractor {

    Integer extractEpisodeNumber(BtVodEntry row);
}
