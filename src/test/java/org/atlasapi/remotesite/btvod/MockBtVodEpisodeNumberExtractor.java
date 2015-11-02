package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

public class MockBtVodEpisodeNumberExtractor implements BtVodEpisodeNumberExtractor {

    public MockBtVodEpisodeNumberExtractor() {}

    public Integer extractEpisodeNumber(BtVodEntry row) {
        return null;
    }
}
