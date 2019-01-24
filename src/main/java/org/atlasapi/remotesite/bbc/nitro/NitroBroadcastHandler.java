package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.reporting.OwlReporter;

import com.metabroadcast.atlas.glycerin.model.Broadcast;

/**
 * <p>
 * A {@code NitroBroadcastHandler} processes a {@link Broadcast}.
 * </p>
 * 
 * @param <T>
 *            - the type of the result of processing the {@code Broadcast}.
 */
public interface NitroBroadcastHandler<T> {

    /**
     * Process {@link Broadcast}.
     * @param broadcast - the {@code Broadcast} to be processed.
     * @return - the result of processing the {@code broadcast}
     */
    T handle(Iterable<Broadcast> broadcast, OwlReporter owlReporter) throws NitroException;

}
