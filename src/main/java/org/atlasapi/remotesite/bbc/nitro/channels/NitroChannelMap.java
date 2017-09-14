package org.atlasapi.remotesite.bbc.nitro.channels;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableBiMap;

import static com.google.common.base.Preconditions.checkNotNull;


public class NitroChannelMap extends ForwardingMap<String, Channel> {

    private ImmutableBiMap<String, Channel> delegate;

    public NitroChannelMap(ChannelResolver resolver) {
        this.delegate = ImmutableBiMap.<String, Channel>builder()
                .put("BBC One Yorkshire", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/yorkshire"))
                .put("BBC One West Midlands", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/west_midlands"))
                .put("BBC One East Midlands", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/east_midlands"))
                .put("BBC One East", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/east"))
                .put("BBC One South East", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/south_east"))
                .put("BBC One South", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/south"))
                .put("BBC One North West", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/north_west"))
                .put("BBC One North East & Cumbria", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/north_east"))
                .put("BBC One West", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/west"))
                .put("BBC One South West", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/south_west"))
                .put("BBC One London", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/london"))
                .put("BBC One Scotland HD", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1776"))
                .put("BBC One Cambridgeshire", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1664"))
                .put("BBC One Scotland", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/scotland"))
                .put("BBC One Channel Islands", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1663"))
                .put("BBC One Northern Ireland HD", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1770"))
                .put("BBC One Oxfordshire", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1662"))
                .put("BBC One Northern Ireland", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/ni"))
                .put("BBC One HD", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/hd"))
                .put("BBC One Wales HD", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1778"))
                .put("BBC One Wales", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/wales"))
                .put("BBC One Yorkshire & Lincolnshire", resolve(resolver, "http://www.bbc.co.uk/services/bbcone/east_yorkshire"))
                .put("BBC Two Northern Ireland", resolve(resolver, "http://www.bbc.co.uk/services/bbctwo/ni"))
                .put("BBC Two Scotland", resolve(resolver, "http://www.bbc.co.uk/services/bbctwo/scotland"))
                .put("BBC Two Wales", resolve(resolver, "http://www.bbc.co.uk/services/bbctwo/wales"))
                .put("BBC Two HD", resolve(resolver, "http://ref.atlasapi.org/channels/pressassociation.com/1782"))
                .put("BBC Two England", resolve(resolver, "http://www.bbc.co.uk/services/bbctwo/england"))
                .build();
    }

    private Channel resolve(ChannelResolver resolver, String uri) {
        return checkNotNull(resolver.fromUri(uri), uri).requireValue();
    }

    @Override
    protected Map<String, Channel> delegate() {
        return delegate;
    }
}
