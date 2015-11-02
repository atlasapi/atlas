package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.io.IOException;
import java.util.Iterator;

public interface BtMpxVodClient {

    Iterator<BtVodEntry> getFeed(String name) throws IOException;

    Optional<BtVodEntry> getItem(String guid);
}
