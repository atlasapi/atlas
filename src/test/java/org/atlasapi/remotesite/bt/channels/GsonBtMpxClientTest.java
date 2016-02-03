package org.atlasapi.remotesite.bt.channels;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.metabroadcast.common.http.FixedResponseHttpClient;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.GsonBtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;

import static com.google.common.base.Predicates.not;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class GsonBtMpxClientTest {

    private static final String baseUri = "http://example.org/1/root";
    private final Gson gson = new GsonBuilder()
            .create();
    private static final Logger LOGGER = LoggerFactory.getLogger(GsonBtMpxClientTest.class);

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
    public void testNewValueDeserialization() throws Exception {
        //Testing the deserialization with a local copy of the file.
        SimpleHttpClient httpClient
                = FixedResponseHttpClient.respondTo(
                baseUri + "?form=cjson",
                Resources.getResource("vole-med-feed-linear.json"));

        BtMpxClient client = new GsonBtMpxClient(httpClient, baseUri);

        PaginatedEntries channels = client.getChannels(Optional.<Selection>absent());

        Iterable<Entry> nonZeroEntries = Iterables.filter(channels.getEntries(),
                not(isZeroAvailableDate(0l)));

        Entry firstNonZeroEntry = Iterables.getFirst(nonZeroEntries, null);

        assertEquals(1446854400000l, firstNonZeroEntry.getAvailableDate());
        assertThat(firstNonZeroEntry.getLinearEpgChannelId(), is("urn:BT:linear:service:750650"));

    }

    @Test
    public void testNewValueDeserializationDirectFromWebsiteWithInternalHttpClient() throws Exception {
        //Testing deserialization with the remote copy of the file from the website.
        SimpleHttpClient httpClient1 = new SimpleHttpClientBuilder().build();
        PaginatedEntries entriesFromRemote = httpClient1.get(createHttpRequestFromVole());

        Iterable<Entry> nonZeroEntries = Iterables.filter(entriesFromRemote.getEntries(),
                not(isZeroAvailableDate(0l)));

        Entry firstNonZeroEntry = Iterables.getFirst(nonZeroEntries, null);

        assertEquals(1446854400000l, firstNonZeroEntry.getAvailableDate());
    }

    @Test
    public void testNewValueDeserializationDirectFromTest2WebsiteWithGsonBtMpxClient() throws BtMpxClientException {
        SimpleHttpClient httpClient
                = new SimpleHttpClientBuilder().build();

        GsonBtMpxClient client = new GsonBtMpxClient(httpClient,
                "http://bt.feed.theplatform.eu/f/kfloDSwm/vole-med-feed-linear");

        PaginatedEntries channels = client.getChannels(Optional.<Selection>absent());
        Iterable<Entry> nonZeroEntries = Iterables.filter(channels.getEntries(),
                not(isZeroAvailableDate(0l)));

        Entry firstNonZeroEntry = Iterables.getFirst(nonZeroEntries, null);

        assertEquals(1446854400000l, firstNonZeroEntry.getAvailableDate());
        assertEquals("urn:BT:linear:service:750650", firstNonZeroEntry.getLinearEpgChannelId());
    }

    @Test
    public void testNewValueDeserializationDirectFromProdWebsiteWithGsonBtMpxClient() throws BtMpxClientException {
        SimpleHttpClient httpClient
                = new SimpleHttpClientBuilder().build();

        GsonBtMpxClient client = new GsonBtMpxClient(httpClient,
                "http://bt.feed.theplatform.eu/f/wzIRPC/btv-med-feed-linear");

        PaginatedEntries channels = client.getChannels(Optional.<Selection>absent());

        Entry firstNonZeroEntry = Iterables.getFirst(channels.getEntries(), null);

        assertNotNull(firstNonZeroEntry.getAvailableDate());
        //This test needs to be updated once prod environment have the linearEpgChannelId field otherwise it will fail.
        assertNull(firstNonZeroEntry.getLinearEpgChannelId());
    }

    private Predicate<Entry> isZeroAvailableDate(final long availableDate) {

        return new Predicate<Entry>() {

            @Override
            public boolean apply(Entry entry) {
                return entry.getAvailableDate() == availableDate;
            }
        };
    }

    private  SimpleHttpRequest<PaginatedEntries> createHttpRequestFromVole() {
        SimpleHttpRequest<PaginatedEntries> httpRequest = SimpleHttpRequest.httpRequestFrom("http://bt.feed.theplatform.eu/f/kfloDSwm/vole-med-feed-linear?form=cjson",
                new HttpResponseTransformer<PaginatedEntries>() {

                    @Override
                    public PaginatedEntries transform(HttpResponsePrologue prologue, InputStream body)
                            throws HttpException, Exception {
                        if (HttpStatusCode.OK.code() == prologue.statusCode()) {
                            return gson.fromJson(new InputStreamReader(body), PaginatedEntries.class);
                        }
                        throw new HttpException(String.format("Request %s failed: %s %s",
                                "http://bt.feed.theplatform.eu/f/kfloDSwm/vole-med-feed-linear?form=cjson", prologue.statusCode(), prologue.statusLine()), prologue);
                    }
                });

        return httpRequest;
    }
}
