package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;

import com.metabroadcast.atlas.glycerin.GlycerinException;

import com.google.common.collect.ImmutableSet;

public interface NitroChannelAdapter {

    ImmutableSet<Channel> fetchServices() throws GlycerinException;

    ImmutableSet<Channel> fetchMasterbrands() throws GlycerinException;

}
