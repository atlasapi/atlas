package org.atlasapi.remotesite.bt.channels;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.atlasapi.remotesite.bt.channels.mpxclient.GsonBtMpxClient;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.metabroadcast.common.http.FixedResponseHttpClient;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.query.Selection;

public class GsonBtMpxClientTest {

    private static final String baseUri = "http://example.org/1/root";
    
    @Test
    public void testDeserialize() throws BtMpxClientException {
        SimpleHttpClient httpClient 
            = FixedResponseHttpClient.respondTo(
                    baseUri + "?form=cjson", 
                    Resources.getResource("media-feed-example.json"));
        
        BtMpxClient client = new GsonBtMpxClient(httpClient, baseUri);
        
        PaginatedEntries channels = client.getChannels(Optional.<Selection>absent());
        
        assertThat(channels.getEntryCount(), is(2));
        assertThat(channels.getStartIndex(), is(1));
        assertThat(channels.getTitle(), is("Media Feed for Linear Channel Availability"));
        
        assertThat(channels.getEntries().size(), is(2));
        Entry firstChannel = Iterables.getFirst(channels.getEntries(), null);
        
        assertThat(firstChannel.getGuid(), is("hkqs"));
        assertThat(firstChannel.getTitle(), is ("BBC One London"));
        assertThat(firstChannel.getCategories().size(), is(4));
        
        Category firstCategory = Iterables.getFirst(firstChannel.getCategories(), null);
        assertThat(firstCategory.getName(), is("S0123456"));
        assertThat(firstCategory.getScheme(), is("subscription"));
        assertThat(firstCategory.getLabel(), is(""));
        
        Content content = Iterables.getOnlyElement(firstChannel.getContent());
        assertThat(Iterables.getOnlyElement(content.getAssetTypes()), is("image-single-packshot"));
        assertThat(content.getSourceUrl(), is("http://img01.bt.co.uk/s/assets/290414/images/bts-logo.png"));
        
        assertTrue(firstChannel.isApproved());
        assertTrue(firstChannel.isStreamable());
        assertTrue(firstChannel.hasOutputProtection());

    }

    @Test
    public void testNewValueDeserialization() throws BtMpxClientException {
        SimpleHttpClient httpClient
                = FixedResponseHttpClient.respondTo(
                baseUri + "?form=cjson",
                Resources.getResource("vole-med-feed-linear.json"));
        BtMpxClient client = new GsonBtMpxClient(httpClient, baseUri);

        PaginatedEntries channels = client.getChannels(Optional.<Selection>absent());

        Entry firstChannel = Iterables.getFirst(channels.getEntries(), null);
        long expectedAvailableDate = Long.valueOf("1446556354000");

        assertEquals(expectedAvailableDate , firstChannel.getAvailableDate());
        assertThat(firstChannel.getLinearEpgChannelId(), is("urn:BT:linear:service:769254"));

    }
}
