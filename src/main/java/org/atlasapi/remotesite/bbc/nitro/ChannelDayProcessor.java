package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.reporting.telescope.OwlTelescopeProxy;

import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Performs an action for the given {@link ChannelDay}.
 */
public interface ChannelDayProcessor {

    UpdateProgress process(ChannelDay channelDay, OwlTelescopeProxy telescope) throws Exception;

}
