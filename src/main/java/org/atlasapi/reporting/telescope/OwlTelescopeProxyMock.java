package org.atlasapi.reporting.telescope;

import java.util.List;
import java.util.Set;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is not for testing.
 * <p>
 * {@link org.atlasapi.remotesite.bbc.nitro.ChannelDayProcessingTask} contains functionality used by
 * multiple ingesters. Some of those ingesters would like to use that functionality with a telescope
 * and some without. Because the TelescopeProxy object trickles down to other commonly used
 * functions, we want to not avoid having to do a null check every time we perform a logging action.
 * Until all Ingesters start using the telescope client, we will use this fake telescope client as a
 * default.
 * <p>
 * Not the prettiest of solutions but if you think of telescope as a logging client it makes more
 * sense. This telescope will log to log files instead of telescope. Good enough, and good for
 * debugging as well.
 */
public class OwlTelescopeProxyMock extends OwlTelescopeProxy {

    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeProxy.class);

    OwlTelescopeProxyMock(Process process) {
        super(process, Event.Type.INGEST);
    }

    public static OwlTelescopeProxy create() {
        Process process = TelescopeUtilityMethodsAtlas.getProcess(OwlTelescopeReporters.MOCK_REPORTER);
        OwlTelescopeProxy telescopeProxy = new OwlTelescopeProxyMock(process);

        return telescopeProxy;
    }

    @Override
    public void startReporting() {
        //we dont want to swarm the debug log, uncomment if you need.
        // log.debug("An atlas report was started through a mock telescope");
    }

    @Override
    public void endReporting() {
        // log.trace("An atlas report has finished through a mock telescope");
    }

    @Override
    public void reportSuccessfulEvent(
            String atlasItemId, List<Alias> aliases, Object objectToSerialise) {
        // log.trace("Someone reported a successful event through a mock telescope. AtlasId={}", atlasItemId);
    }

    @Override
    public void reportSuccessfulEvent(
            long dbId,
            Set<org.atlasapi.media.entity.Alias> aliases,
            Object objectToSerialise) {
        reportSuccessfulEvent(
                encode(dbId),
                TelescopeUtilityMethodsAtlas.getAliases(aliases),
                objectToSerialise
        );
    }

    @Override
    public void reportFailedEventWithWarning(
            String atlasItemId, String warningMsg, Object objectToSerialise) {
        // log.trace("Someone reported a failed event through a mock telescope. AtlasId={}", atlasItemId);

    }

    @Override
    public void reportFailedEventWithError(String errorMsg, Object objectToSerialise) {
        // log.trace("Someone reported a failed event through a mock telescope.");
    }
}
