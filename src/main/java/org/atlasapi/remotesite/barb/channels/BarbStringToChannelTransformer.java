package org.atlasapi.remotesite.barb.channels;

import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.PublisherDetails;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.atlasapi.media.entity.Publisher.BARB_CHANNELS;

public class BarbStringToChannelTransformer {

    private static final String ALIAS_NAMESPACE = "gb:barb:stationCode";
    private static final String URI_NAMESPACE = "uri";
    private static final String URI_FORMAT = "http://%s/channels/%s";

    private final PublisherDetails PUB_DETAILS = new PublisherDetails(BARB_CHANNELS.key());

    private final ChannelModelTransformer modelTransformer;

    public static BarbStringToChannelTransformer create(ChannelModelTransformer modelTransformer) {
        return new BarbStringToChannelTransformer(modelTransformer);
    }

    private BarbStringToChannelTransformer(ChannelModelTransformer modelTransformer) {
        this.modelTransformer = checkNotNull(modelTransformer);
    }

    public org.atlasapi.media.channel.Channel transform(String str) {
        String[] split = str.split(",");

        checkArgument(split.length == 2);

        String stationCode = trim(split[0]);
        String channelTitle = sanitizeTitle(split[1]);

        checkArgument(isValid(stationCode));

        Channel channel = new Channel();

        channel.setTitle(channelTitle);
        channel.setUri(uriFor(stationCode));
        channel.setPublisherDetails(PUB_DETAILS);
        channel.setChannelType("CHANNEL");
        channel.setMediaType("video");

        org.atlasapi.media.channel.Channel complex =  modelTransformer.transform(channel);
        complex.addAlias(new Alias(ALIAS_NAMESPACE, stationCode)); // The model transformer does not transform aliases. Bug? 18/06/18
        complex.addAlias(new Alias(URI_NAMESPACE, complex.getUri()));

        return complex;
    }

    private String uriFor(String code) {
        return format(URI_FORMAT, PUB_DETAILS.getKey(), code);
    }

    private String trim(String str) {
        return str.trim();
    }

    private String sanitizeTitle(String title) {
        return trim(title.replace("\\",""));
    }

    private boolean isValid(String stationCode) {
        return stationCode.matches("^\\d+");
    }

}
