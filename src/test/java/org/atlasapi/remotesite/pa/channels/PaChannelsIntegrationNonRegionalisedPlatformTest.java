package org.atlasapi.remotesite.pa.channels;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class PaChannelsIntegrationNonRegionalisedPlatformTest extends TestCase {

    private PaChannelsIngester channelsIngester;
    private PaChannelGroupsIngester channelGroupsIngester;
    private PaChannelDataHandler dataHandler;
    private File file = new File("src/test/resources/", "201212141542_tv_channel_data.xml");
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
    public void testNonRegionalisedPlatformIngest() {
        Optional<ChannelGroup> maybePlatform = channelGroupStore.fromAlias("http://pressassociation.com/platforms/3");
        assertTrue(maybePlatform.isPresent());
        assertTrue(!maybePlatform.get().getChannelNumberings().isEmpty());
        Platform platform = (Platform)maybePlatform.get();
        assertTrue(platform.getRegions().isEmpty());
    }
}
