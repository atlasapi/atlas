package org.atlasapi.remotesite.bbc.ion;

import static org.atlasapi.persistence.logging.AdapterLogEntry.warnEntry;

import java.util.Set;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentStore;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.ContentLock;
import org.atlasapi.remotesite.bbc.ion.model.IonBroadcast;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;

public class OndemandBbcIonBroadcastHandler implements BbcIonBroadcastHandler {

    private final ContentStore store;
    private final AdapterLog log;
    private final ContentLock lock;

    public OndemandBbcIonBroadcastHandler(ContentStore store, AdapterLog log, ContentLock lock) {
        this.store = store;
        this.log = log;
        this.lock = lock;
    }

    @Override
    public Maybe<ItemAndPossibleBroadcast> handle(IonBroadcast broadcast) {
        try {
            Maybe<Item> item = tryHandle(broadcast);
            if(item.hasValue()) {
                return Maybe.just(new ItemAndPossibleBroadcast(item.requireValue(), Optional.<Broadcast>absent()));
            }
        } catch (Exception e) {
            log.record(warnEntry().withSource(getClass()).withCause(e).withDescription("Failed to process ondemand for %s", broadcast.getEpisodeId()));
        }
        return Maybe.nothing();
    }

    private Maybe<Item> tryHandle(IonBroadcast broadcast) {

        String itemId = broadcast.getEpisodeId();
        String itemUri = BbcFeeds.slashProgrammesUriForPid(itemId);
        Item item = null;
        
        try {
            lock.lock(itemUri);
            item = resolve(itemUri);
            if (item == null) {
                return Maybe.nothing();
            }
    
            Version version = findVersion(item, BbcFeeds.slashProgrammesUriForPid(broadcast.getVersionId()));
            if (version == null) {
                log.record(warnEntry().withSource(getClass()).withDescription("No version %s for %s", broadcast.getVersionId(), broadcast.getEpisodeId()));
                return Maybe.nothing();
            }
    
            String iplayerId = iplayerId(itemId);
    
            Encoding encoding = findEncoding(version, iplayerId);
    
            DateTime actualStart = broadcast.getEpisode().getActualStart();
            DateTime availableUntil = broadcast.getEpisode().getAvailableUntil();
    
            if ("CURRENT".equals(broadcast.getEpisode().getAvailability()) && actualStart != null && availableUntil != null) {
                Location location = null;
    
                if (encoding == null) {
                    encoding = new Encoding();
                    encoding.setCanonicalUri(iplayerId);
                    version.addManifestedAs(encoding);
    
                    location = new Location();
                    encoding.addAvailableAt(location);
                }
                location = Iterables.getOnlyElement(encoding.getAvailableAt());
    
    
                updateLocation(location, actualStart, availableUntil, itemId);
            } else {
                if (encoding != null) {
                    Set<Encoding> encodings = Sets.newHashSet(version.getManifestedAs());
                    encodings.remove(encoding);
                    version.setManifestedAs(encodings);
                }
            }
    
            store.writeContent(item);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        finally {
            lock.unlock(itemUri);
        }

        return Maybe.just(item);
    }

    private void updateLocation(Location location, DateTime actualStart, DateTime availableUntil, String itemId) {
        Policy policy = new Policy();
        policy.setAvailabilityStart(actualStart);
        policy.setAvailabilityEnd(availableUntil);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        location.setPolicy(policy);
        location.setCanonicalUri(iplayerId(itemId));

        location.setTransportType(TransportType.LINK);
        location.setUri("http://www.bbc.co.uk/iplayer/episode/" + itemId);
    }

    private Encoding findEncoding(Version version, String iplayerId) {
        for (Encoding encoding : version.getManifestedAs()) {
            if (iplayerId.equals(encoding.getCanonicalUri())) {
                return encoding;
            }
        }
        return null;
    }

    private String iplayerId(String itemId) {
        return String.format("iplayer:" + itemId);
    }

    private Version findVersion(Item item, String versionId) {
        for (Version version : item.getVersions()) {
            if (versionId.equals(version.getCanonicalUri())) {
                return version;
            }
        }
        return null;
    }

    private Item resolve(String uri) {
        Alias itemProgrammesUrl = new Alias("bbc:programmes:url", uri);
        Optional<Content> maybeItem = store.resolveAliases(ImmutableList.of(itemProgrammesUrl), Publisher.BBC).get(itemProgrammesUrl);
        if (maybeItem.isPresent()) {
            log.record(warnEntry().withSource(getClass()).withDescription("Couldn't resolve item for %s", uri));
            return null;
        }
        Identified resolved = maybeItem.get();
        if (!(resolved instanceof Item)) {
            log.record(warnEntry().withSource(getClass()).withDescription("Resolved %s not item for %s", resolved.getClass().getSimpleName(), uri));
            return null;
        }
        return (Item) resolved;
    }

}
