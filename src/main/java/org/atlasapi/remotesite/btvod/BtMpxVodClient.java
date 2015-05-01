package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.io.IOException;
import java.util.Iterator;

public interface BtMpxVodClient {

    Iterator<BtVodEntry> getBtMpxFeed() throws IOException;
}
