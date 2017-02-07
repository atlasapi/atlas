package org.atlasapi.remotesite.five;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.system.RemoteSiteClient;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpResponse;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class FiveBrandWatchablesProcessorTest {

    private FiveBrandWatchablesProcessor processor;
    private ArgumentCaptor<Container> containerArgumentCaptor;
    private ArgumentCaptor<Item> itemArgumentCaptor;
    private ContentWriter writer;

    public FiveBrandWatchablesProcessorTest() {
        containerArgumentCaptor = ArgumentCaptor.forClass(Container.class);
        itemArgumentCaptor = ArgumentCaptor.forClass(Item.class);

        writer = mock(ContentWriter.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);
        ContentResolver contentResolver = mock(ContentResolver.class);
        String baseApiUrl = "";
        RemoteSiteClient<HttpResponse> httpClient = (RemoteSiteClient<HttpResponse>) mock(
                RemoteSiteClient.class
        );

        FiveLocationPolicyIds locationPolicyIds = mock(FiveLocationPolicyIds.class);

        when(contentResolver.findByCanonicalUris(
                any( ImmutableSet.class)
        )).thenReturn(ResolvedContent.builder().build());

        Channel channel = Channel.builder()
                .withInteractive(true)
                .withUri("someuri")
                .withChannelNumber(
                        ChannelNumbering.builder()
                                .withChannelNumber("22")
                                .withChannelGroup(mock(ChannelGroup.class))
                                .build()
                )
                .build();
        when(channelResolver.fromUri(any(String.class)))
                .thenReturn(Maybe.just(channel));

        Multimap<String, Channel> channelMap = new FiveChannelMap(channelResolver);

        try {
            when(httpClient.get(
                    any(String.class)
            )).thenReturn(HttpResponse.sucessfulResponse(makeHttpBodyResponse()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        processor = FiveBrandWatchablesProcessor.builder()
                .withWriter(writer)
                .withContentResolver(contentResolver)
                .withBaseApiUrl(baseApiUrl)
                .withHttpClient(httpClient)
                .withChannelMap(channelMap)
                .withLocationPolicyIds(locationPolicyIds)
                .build();
    }

    private String makeHttpBodyResponse() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(
                    "src/main/java/org/atlasapi/remotesite/five/fiveShowExampleData")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        StringBuilder sb = new StringBuilder();

        try {
            while((line = br.readLine())!= null){
                sb.append(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    @Test
    public void processEpisode() {
        Builder builder = new Builder();
        Document document = null;
        try {
            document = builder.build("src/main/java/org/atlasapi/remotesite/five/fiveTestXmlData");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Element element = document.getRootElement()
                .getChildElements("transmissions").get(0).getFirstChildElement("transmission");

        processor.processShow(element);

        verify(writer, times(3)).createOrUpdate(containerArgumentCaptor.capture());
        verify(writer, times(14)).createOrUpdate(itemArgumentCaptor.capture());

        Item expectedItem = makeExpectedItem();
        Container expectedContainer = makeExpectedContainer();

        List<Item> capturedItems = itemArgumentCaptor.getAllValues();
        List<Container> capturedContainers = containerArgumentCaptor.getAllValues();
        assertEquals(expectedContainer, capturedContainers.get(0));
        assertEquals(expectedItem, capturedItems.get(0));
    }

    private Item makeExpectedItem() {
        Item item = new Item();
        item.setCanonicalUri("/watchables/C5203540001");
        return item;
    }

    private Container makeExpectedContainer() {
        Container container = new Container();
        container.setCanonicalUri("/shows/3937285830");

        return container;
    }
}