package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

public class MockBtVodEpisodeNumberExtractor extends BtVodEpisodeNumberExtractor{

    public MockBtVodEpisodeNumberExtractor(BtMpxVodClient mpxClient) {
        super(mpxClient);
    }

    @Override
    public Integer extractEpisodeNumber(BtVodEntry row) {
        return null;
    }
}
