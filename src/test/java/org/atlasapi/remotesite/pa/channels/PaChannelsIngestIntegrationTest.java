    package org.atlasapi.remotesite.pa.channels;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.net.ftp.FTPFile;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class PaChannelsIngestIntegrationTest extends TestCase {

    private PaChannelsIngester channelsIngester;
    private PaChannelGroupsIngester channelGroupsIngester;
    private PaChannelDataHandler dataHandler;
    private File file = new File("src/test/resources/", "201212141541_tv_channel_data.xml");
    private ChannelStore channelStore;
    private ChannelGroupStore channelGroupStore;
    private PaProgrammeDataStore store = new DummyPaProgrammeDataStore(file);
    private PaChannelsUpdater updater; 
    private Lock lock = new ReentrantLock();
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DatabasedMongo db = MongoTestHelper.anEmptyTestDatabase();
        channelGroupStore = new MongoChannelGroupStore(db);
        channelStore = new MongoChannelStore(db, channelGroupStore, channelGroupStore);
        channelsIngester = new PaChannelsIngester();
        channelGroupsIngester = new PaChannelGroupsIngester();
        dataHandler = new PaChannelDataHandler(channelsIngester, channelGroupsIngester, channelStore, channelStore, channelGroupStore, channelGroupStore);
        updater = new PaChannelsUpdater(store, dataHandler, lock);
        updater.run();
    }
    
    @Test
    public void testChannelsIngest() {
        Maybe<Channel> maybeChannel = channelStore.forAlias("http://pressassociation.com/channels/1");
        assertTrue(maybeChannel.hasValue());
        Channel channel = maybeChannel.requireValue();
        assertEquals("BBC One Northern Ireland", channel.getTitle());
        // test parent is correct
        Long parent = channel.getParent();
        assertNotNull(parent);
        Maybe<Channel> maybeParent = channelStore.fromId(parent);
        assertTrue(maybeParent.hasValue());
        assertEquals("BBC One", maybeParent.requireValue().getTitle());
        // test availableFrom is set correctly to PA
        ImmutableSet<Publisher> expectedAvailableFrom = ImmutableSet.of(Publisher.PA);
        assertEquals(expectedAvailableFrom, maybeParent.requireValue().getAvailableFrom());
        assertEquals(expectedAvailableFrom, maybeChannel.requireValue().getAvailableFrom());
        
        // check numbering
        ChannelNumbering numbering = Iterables.getOnlyElement(channel.getChannelNumbers());
        assertEquals(numbering.getChannelNumber(), "1");
        assertEquals(channel.getId(), numbering.getChannel());
        
        // get region
        Optional<ChannelGroup> maybeRegion = channelGroupStore.channelGroupFor(numbering.getChannelGroup()); 
        assertTrue(maybeRegion.isPresent());
        Region region = (Region)maybeRegion.get();
        assertEquals("Northern Ireland", region.getTitle());
        // get platform
        Optional<ChannelGroup> maybePlatform = channelGroupStore.channelGroupFor(region.getPlatform());
        assertTrue(maybePlatform.isPresent());
        Platform platform = (Platform)maybePlatform.get();
        assertEquals("Freeview", platform.getTitle());
    }
    
    // test platform has no channels
    @Test
    public void testNoChannelsOnPlatform() {
        Optional<ChannelGroup> maybePlatform = channelGroupStore.fromAlias("http://pressassociation.com/platforms/3");
        assertTrue(maybePlatform.isPresent());
        assertTrue(maybePlatform.get().getChannelNumberings().isEmpty());
    }
    
    // test regions have unique channels, but some are on all regions
    @Test
    public void testRegionalisedChannels() {
        Optional<ChannelGroup> maybeRegion = channelGroupStore.fromAlias("http://pressassociation.com/regions/3-56");
        assertTrue(maybeRegion.isPresent());
        Region niRegion = (Region)maybeRegion.get();
        
        Set<ChannelNumbering> numberings = niRegion.getChannelNumberings();
        
        // bbc 4 on multiple regions
        // channel id 742, channel # 9
        Maybe<Channel> maybeChannel = channelStore.forAlias("http://pressassociation.com/channels/742");
        Channel bbcFour = maybeChannel.requireValue();
        
        // bbc one ni digital on ni region only
        // channel id 414, # 1
        maybeChannel = channelStore.forAlias("http://pressassociation.com/channels/1");
        Channel bbcNi = maybeChannel.requireValue();
        
        // NI has BBC One NI Digital and BBC Four
        boolean bbcFourPresent = false;
        boolean bbcOneNiPresent = false;
        for (ChannelNumbering numbering : numberings) {
            if (numbering.getChannel().equals(bbcFour.getId())
                && numbering.getChannelNumber().equals("9")) {
                bbcFourPresent = true;
            }
            if (numbering.getChannel().equals(bbcNi.getId())
                    && numbering.getChannelNumber().equals("1")) {
                    bbcOneNiPresent = true;
                }
        }
        
        assertTrue(bbcFourPresent);
        assertTrue(bbcOneNiPresent);
        
        maybeRegion = channelGroupStore.fromAlias("http://pressassociation.com/regions/3-55");
        assertTrue(maybeRegion.isPresent());
        Region nWRegion = (Region)maybeRegion.get();
        
        numberings = nWRegion.getChannelNumberings();
        
        // North West has BBC Four, but not BBC One NI Digital
        bbcFourPresent = false;
        bbcOneNiPresent = false;
        for (ChannelNumbering numbering : numberings) {
            if (numbering.getChannel().equals(bbcFour.getId())
                && numbering.getChannelNumber().equals("9")) {
                bbcFourPresent = true;
            }
            if (numbering.getChannel().equals(bbcNi.getId())
                    && numbering.getChannelNumber().equals("1")) {
                    bbcOneNiPresent = true;
                }
        }

        assertTrue(bbcFourPresent);
        assertFalse(bbcOneNiPresent);
    }
}

class DummyPaProgrammeDataStore implements PaProgrammeDataStore {

    File file;
    
    public DummyPaProgrammeDataStore(File file) {
        this.file = file;
    }

    @Override
    public boolean requiresUpdating(FTPFile file) {
        throw new NotImplementedException();
    }

    @Override
    public void save(String fileName, InputStream dataStream) throws Exception {
        throw new NotImplementedException();
        
    }

    @Override
    public List<File> localTvDataFiles(Predicate<File> filter) {
        throw new NotImplementedException();
    }

    @Override
    public List<File> localFeaturesFiles(Predicate<File> filter) {
        throw new NotImplementedException();
    }

    @Override
    public List<File> localChannelsFiles(Predicate<File> filter) {
        return ImmutableList.of(file);
    }

    @Override
    public File copyForProcessing(File file) {
        return file;
    }

    @Override
    public List<File> localProfilesFiles(Predicate<File> filter) {
        throw new NotImplementedException();
    }
}
