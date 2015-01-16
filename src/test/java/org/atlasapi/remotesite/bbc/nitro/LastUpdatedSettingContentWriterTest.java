package org.atlasapi.remotesite.bbc.nitro;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.TimeMachine;


@RunWith(MockitoJUnitRunner.class)
public class LastUpdatedSettingContentWriterTest {

    private static final String VERSION_URI = "versionCanonicalUri";
    private static final String ITEM_URI = "itemCanonicalUri";

    private static final String ENCODING_1_URI = "encoding1Uri";
    private static final String ENCODING_2_URI = "encoding2Uri";
    private static final String ENCODING_3_URI = "encoding3Uri";

    private static final String BROADCAST_1_ON = "broadcast1On";
    private static final String BROADCAST_2_ON = "broadcast2On";
    private static final String BROADCAST_3_ON = "broadcast3On";

    private static final String BROADCAST_1_SOURCE = "broadcast1Source";
    private static final String BROADCAST_2_SOURCE = "broadcast2Source";
    private static final String BROADCAST_3_SOURCE = "broadcast3Source";

    private static final DateTime NOW = DateTime.parse("2014-12-16T17:10");
    private static final DateTime FUTURE = DateTime.parse("2015-01-16T10:00");
    private static final DateTime PAST = DateTime.parse("2014-11-15T10:00");

    @Mock
    private ContentResolver resolver;
    @Mock
    private ContentWriter writer;

    private static final TimeMachine clock = new TimeMachine();
    private LastUpdatedSettingContentWriter lastUpdatedWriter;

    @Before
    public void setup() {
        clock.jumpTo(NOW);
        lastUpdatedWriter = new LastUpdatedSettingContentWriter(resolver, writer, clock);
    }

    @Test
    public void testLastUpdatedTimeShouldNotChangeWhenItemDoesntChange() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(previousItem.getLastUpdated(), currentItem.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldChangeWhenItemChanges() {
        Item previousItem = item();

        Item currentItem = item();
        currentItem.setBlackAndWhite(false);

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(NOW, currentItem.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldNotChangeWhenVersionDoesntChange() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        lastUpdatedWriter.createOrUpdate(currentItem);
        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        Version previousVersion = byCanonicalUri(previousItem.getVersions(), VERSION_URI);

        assertEquals(previousVersion.getLastUpdated(), currentVersion.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldChangeWhenVersionChanges() {
        Item previousItem = item();
        Item currentItem = item();

        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        currentVersion.setDuration(Duration.standardMinutes(90));

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(NOW, currentVersion.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldNotChangeWhenBroadcastDoesntChange() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        Broadcast currentBroadcast = bySourceId(currentVersion.getBroadcasts(), BROADCAST_1_SOURCE);

        Version previousVersion = byCanonicalUri(previousItem.getVersions(), VERSION_URI);
        Broadcast previousBroadcast = bySourceId(previousVersion.getBroadcasts(), BROADCAST_1_SOURCE);

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(previousBroadcast.getLastUpdated(), currentBroadcast.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldChangeWhenBroadcastChanges() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        Broadcast currentBroadcast = bySourceId(currentVersion.getBroadcasts(), BROADCAST_1_SOURCE);
        currentBroadcast.setActualTransmissionEndTime(FUTURE);

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(NOW, currentBroadcast.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldNotChangeWhenEncodingDoesntChange() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        Encoding currentEncoding = Iterables.getFirst(currentVersion.getManifestedAs(), null);

        Version previousVersion = byCanonicalUri(previousItem.getVersions(), VERSION_URI);
        Encoding previousEncoding = Iterables.getFirst(previousVersion.getManifestedAs(), null);

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(previousEncoding.getLastUpdated(), currentEncoding.getLastUpdated());
    }

    @Test
    public void testLastUpdatedTimeShouldChangeWhenEncodingChanges() {
        Item previousItem = item();
        Item currentItem = item();

        when(resolver.findByCanonicalUris(ImmutableList.of(ITEM_URI)))
                .thenReturn(resolvedContent(previousItem));

        Version currentVersion = byCanonicalUri(currentItem.getVersions(), VERSION_URI);
        Encoding currentEncoding = Iterables.getFirst(currentVersion.getManifestedAs(), null);
        currentEncoding.setAudioBitRate(12000);

        Version previousVersion = byCanonicalUri(previousItem.getVersions(), VERSION_URI);
        Encoding previousEncoding = Iterables.getFirst(previousVersion.getManifestedAs(), null);

        lastUpdatedWriter.createOrUpdate(currentItem);

        assertEquals(NOW, currentEncoding.getLastUpdated());
    }

    private ResolvedContent resolvedContent(Item item) {
        return new ResolvedContent(
                ImmutableMap.of(item.getCanonicalUri(), Maybe.<Identified>just(item))
        );
    }

    private Item item() {
        Item item = new Item();
        item.setCanonicalUri(ITEM_URI);
        item.setBlackAndWhite(true);
        item.setTitle("Really nice item");
        item.setLastUpdated(PAST);
        item.setVersions(Sets.newHashSet(version()));
        return item;
    }

    private Version version() {
        Version version = new Version();

        version.setCanonicalUri(VERSION_URI);
        version.setDuration(Duration.standardMinutes(30));
        version.set3d(true);

        version.setBroadcasts(broadcasts());
        version.setManifestedAs(encodings());
        version.setLastUpdated(PAST);

        return version;
    }

    private Set<Encoding> encodings() {
        return ImmutableSet.of(
                encoding(ENCODING_1_URI, true),
                encoding(ENCODING_2_URI, false)
        );
    }

    private Set<Broadcast> broadcasts() {
        return ImmutableSet.of(
                broadcast(BROADCAST_1_ON, DateTime.parse("2014-12-23T10:00"), BROADCAST_1_SOURCE),
                broadcast(BROADCAST_2_ON, DateTime.parse("2014-12-24T10:00"), BROADCAST_2_SOURCE),
                broadcast(BROADCAST_3_ON, DateTime.parse("2014-12-25T10:00"), BROADCAST_3_SOURCE)
        );
    }

    private Broadcast broadcast(String broadcastOn, DateTime startTime, String source) {
        Broadcast broadcast = new Broadcast(broadcastOn, startTime, startTime.plusMinutes(30))
                .withId(source);

        broadcast.setSigned(true);
        broadcast.setAudioDescribed(true);
        broadcast.setLastUpdated(PAST);

        return broadcast;
    }

    private Encoding encoding(String canonicalUri, boolean isHd) {
        Encoding encoding = new Encoding();

        encoding.setAudioBitRate(101010);
        encoding.setSigned(true);
        encoding.setVideoHorizontalSize(isHd ? 1200 : 640);
        encoding.setVideoVerticalSize(isHd ? 900 : 480);
        encoding.setLastUpdated(PAST);

        return encoding;
    }

    private <T extends Identified> T byCanonicalUri(Iterable<T> elements, final String canonicalUri) {
        return Iterables.tryFind(elements, new Predicate<T>() {
            @Override public boolean apply(T input) {
                return canonicalUri.equals(input.getCanonicalUri());
            }
        }).get();
    }

    private Broadcast bySourceId(Iterable<Broadcast> broadcasts, final String sourceId) {
        return Iterables.tryFind(broadcasts, new Predicate<Broadcast>() {
            @Override public boolean apply(Broadcast input) {
                return sourceId.equals(input.getSourceId());
            }
        }).get();
    }

}