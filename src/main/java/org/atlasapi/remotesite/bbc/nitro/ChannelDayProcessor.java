package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Performs an action for the given {@link ChannelDay}.
 */
public interface ChannelDayProcessor {

    UpdateProgress process(ChannelDay channelDay, OwlTelescopeReporter telescope) throws Exception;

}
