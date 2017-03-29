package org.atlasapi.remotesite.five;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.persistence.system.RemoteSiteClient;

import com.metabroadcast.common.http.HttpResponse;

import com.google.common.collect.Multimap;
import org.apache.http.impl.client.CloseableHttpClient;

public class FiveWatchableProcessor extends FiveShowProcessor {

    protected FiveWatchableProcessor(
            String baseApiUrl,
            CloseableHttpClient httpClient,
            Multimap<String, Channel> channelMap,
            FiveLocationPolicyIds locationPolicyIds
    ) {
        super(
                baseApiUrl,
                httpClient,
                channelMap,
                locationPolicyIds
        );
    }

    public static FiveWatchableProcessor create(
            String baseApiUrl,
            CloseableHttpClient httpClient,
            Multimap<String, Channel> channelMap,
            FiveLocationPolicyIds locationPolicyIds
    ) {
        return new FiveWatchableProcessor(
                baseApiUrl,
                httpClient,
                channelMap,
                locationPolicyIds
        );
    }
}
