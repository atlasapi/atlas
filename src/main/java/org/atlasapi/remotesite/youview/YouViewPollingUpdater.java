package org.atlasapi.remotesite.youview;

public class YouViewPollingUpdater extends YouViewUpdater {

    public YouViewPollingUpdater(YouViewChannelResolver channelResolver, YouViewScheduleFetcher fetcher, YouViewChannelProcessor processor, YouViewIngestConfiguration ingestConfiguration, int hours) {
        super(channelResolver, fetcher, processor, ingestConfiguration, 0, 0, true, hours);
    }

}
