package org.atlasapi.remotesite.pa.channels;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.Unmarshaller.Listener;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.remotesite.pa.channels.bindings.Station;
import org.atlasapi.remotesite.pa.channels.bindings.Stations;
import org.atlasapi.remotesite.pa.channels.bindings.TvChannelData;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.google.common.collect.Iterables;
import com.google.common.io.Resources;


public class AdultFlagIngestTest {
    
    private static final String TEST_FILENAME = "tv_channel_data_adult_flag_ingest_test.xml";
    
    private final PaChannelsIngester ingester = new PaChannelsIngester();

    @Test
    public void testIngestOfStationWithoutFlag() throws SAXException, ParserConfigurationException, IOException, JAXBException {
        SingleStationListener listener = new SingleStationListener(ingester, "1");
        readAndParseFile(listener);
        ChannelTree channelTree = listener.getChannelTree();
        
        Channel parent = channelTree.getParent();
        List<Channel> children = channelTree.getChildren();
        
        assertNull(parent.getAdult());
        for (Channel child : children) {
            assertNull(child.getAdult());
        }
    }

    @Test
    public void testIngestOfAdultStationWithSingleChild() throws SAXException, ParserConfigurationException, IOException, JAXBException {
        SingleStationListener listener = new SingleStationListener(ingester, "53");
        readAndParseFile(listener);
        ChannelTree channelTree = listener.getChannelTree();
        
        Channel parent = channelTree.getParent();
        List<Channel> children = channelTree.getChildren();
        
        assertNull(parent);
        Channel child = Iterables.getOnlyElement(children);
        assertTrue(child.getAdult());
    }

    @Test
    public void testIngestOfAdultStationWithMultipleChildren() throws SAXException, ParserConfigurationException, IOException, JAXBException {
        SingleStationListener listener = new SingleStationListener(ingester, "12");
        readAndParseFile(listener);
        ChannelTree channelTree = listener.getChannelTree();
        
        Channel parent = channelTree.getParent();
        List<Channel> children = channelTree.getChildren();
        
        assertTrue(parent.getAdult());
        for (Channel child : children) {
            assertTrue(child.getAdult());
        }
    }

    private void readAndParseFile(Listener listener) throws SAXException, ParserConfigurationException, IOException, JAXBException {
        JAXBContext context = JAXBContext.newInstance("org.atlasapi.remotesite.pa.channels.bindings");
        Unmarshaller unmarshaller = context.createUnmarshaller();

        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        XMLReader reader = factory.newSAXParser().getXMLReader();
        reader.setContentHandler(unmarshaller.getUnmarshallerHandler());
        unmarshaller.setListener(listener);
        reader.parse(new InputSource(readFile(TEST_FILENAME)));
    }
    
    private InputStream readFile(String fileName) throws IOException {
        URL testFile = Resources.getResource(getClass(), fileName);
        return Resources.newInputStreamSupplier(testFile).getInput();
    }
    
    private static class SingleStationListener extends Listener {
        
        private final PaChannelsIngester ingester;
        private final String stationId;
        
        private ChannelTree channelTree;
        
        public SingleStationListener(PaChannelsIngester ingester, String stationId) {
            this.ingester = checkNotNull(ingester);
            this.stationId = checkNotNull(stationId);
        }
        
        public ChannelTree getChannelTree() {
            return channelTree;
        }
        
        @Override
        public void beforeUnmarshal(Object target, Object parent) {
        }

        @Override
        public void afterUnmarshal(Object target, Object parent) {
            if (target instanceof TvChannelData) {
                TvChannelData channelData = (TvChannelData) target;
                
                Stations stations = channelData.getStations();
                for (Station station : stations.getStation()) {
                    if (stationId.equals(station.getId())) {
                        channelTree = ingester.processStation(
                                station, 
                                channelData.getServiceProviders().getServiceProvider()
                        );
                    }
                }
            }
        }
    }
}
