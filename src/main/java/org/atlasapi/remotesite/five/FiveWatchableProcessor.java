package org.atlasapi.remotesite.five;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.persistence.system.RemoteSiteClient;

import com.metabroadcast.common.http.HttpResponse;

import com.google.common.collect.Multimap;

public class FiveWatchableProcessor extends FiveShowProcessor {

    protected FiveWatchableProcessor(
            String baseApiUrl,
            RemoteSiteClient<HttpResponse> httpClient,
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
            RemoteSiteClient<HttpResponse> httpClient,
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
