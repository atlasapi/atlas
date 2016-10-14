package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;

import com.metabroadcast.atlas.glycerin.GlycerinException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public interface NitroChannelAdapter {

    ImmutableList<Channel> fetchServices(ImmutableMap<String, Channel> build) throws GlycerinException;

    ImmutableList<Channel> fetchServices() throws GlycerinException;

    ImmutableSet<Channel> fetchMasterbrands() throws GlycerinException;

}