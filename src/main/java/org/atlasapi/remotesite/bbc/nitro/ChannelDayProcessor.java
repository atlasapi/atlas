package org.atlasapi.remotesite.bbc.nitro;

import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.reporting.OwlReporter;

/**
 * Performs an action for the given {@link ChannelDay}.
 */
public interface ChannelDayProcessor {

    UpdateProgress process(ChannelDay channelDay, OwlReporter owlReporter) throws Exception;

}
