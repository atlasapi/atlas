package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.reporting.OwlReporter;

import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Performs an action for the given {@link ChannelDay}.
 */
public interface ChannelDayProcessor {

    UpdateProgress process(ChannelDay channelDay, OwlReporter owlReporter) throws Exception;

}
