package org.atlasapi.query.v2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Optional;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.content.merge.BroadcastMerger;
import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.remotesite.util.OldContentDeactivator;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;

import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.springframework.http.HttpStatus;
import org.springframework.mock.web.DelegatingServletInputStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;


@RunWith(MockitoJUnitRunner.class)
public class ContentWriteControllerTest {

    private ContentWriteController controller;

    private @Captor ArgumentCaptor<Content> contentCaptor;
    private @Captor ArgumentCaptor<String> locationCaptor;
    private @Captor ArgumentCaptor<ContentWriteMessage> messageCaptor;
    private @Captor ArgumentCaptor<InputStream> streamCaptor;

    private @Mock ApplicationFetcher applicationFetcher;
    private @Mock ContentWriteExecutor writeExecutor;
    private @Mock LookupBackedContentIdGenerator idGenerator;
    private @Mock MessageSender<ContentWriteMessage> messageSender;
    private @Mock LookupEntryStore lookupEntryStore;
    private @Mock ContentResolver contentResolver;
    private @Mock ContentWriter contentWriter;
    private @Mock EquivalenceBreaker equivalenceBreaker;

    private @Mock HttpServletRequest request;
    private @Mock HttpServletResponse response;
    private @Mock AtlasModelWriter modelWriter;
    private @Mock ServletOutputStream outputStream;

    private @Mock Iterator<LookupEntry> entryStoreIterator;
    private @Mock Iterable<LookupEntry> lookupEntries;
    private @Mock LookupEntry lookupEntry;
    private @Mock ResolvedContent resolvedContent;
    private @Mock Maybe<Identified> identifiedMaybe;
    private @Mock Identified identified;
    private @Mock Described described;
    private @Mock SubstitutionTableNumberCodec substitutionTableNumberCodec;
    private @Mock OldContentDeactivator oldContentDeactivator;

    private NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private byte[] inputBytes;
    private ContentWriteExecutor.InputContent inputContent;
    private long contentId;

    private String id = "id";
    private String uri = "uri";

    @Before
    public void setUp() throws Exception {

        Application application = mock(Application.class);
        ApplicationConfiguration configuration = ApplicationConfiguration.builder()
                .withPrecedence(ImmutableList.of(Publisher.METABROADCAST))
                .withEnabledWriteSources(ImmutableList.of(Publisher.METABROADCAST))
                .build();

        when(application.getConfiguration()).thenReturn(configuration);

        inputBytes = "{ content: {} }".getBytes();
        DelegatingServletInputStream inputStream = new DelegatingServletInputStream(
                new ByteArrayInputStream(inputBytes)
        );
        Item content = new Item();
        content.setCanonicalUri(uri);
        content.setPublisher(Publisher.METABROADCAST);
        inputContent = ContentWriteExecutor.InputContent.create(content, "item");
        contentId = 0L;

        when(applicationFetcher.applicationFor(request)).thenReturn(Optional.of(application));
        when(request.getInputStream()).thenReturn(inputStream);
        when(writeExecutor.parseInputStream(any(InputStream.class), anyBoolean()))
                .thenReturn(inputContent);
        when(idGenerator.getId(any(Content.class))).thenReturn(contentId);
        when(response.getOutputStream()).thenReturn(outputStream);

        when(lookupEntry.uri()).thenReturn(uri);
        ImmutableList<LookupEntry> entryList = ImmutableList.of(lookupEntry);
        when(lookupEntryStore.entriesForIds(anyList()))
                .thenReturn(entryList);

        when(contentResolver.findByCanonicalUris(Lists.newArrayList(uri)))
                .thenReturn(resolvedContent);

        controller = ContentWriteController.create(
                applicationFetcher,
                writeExecutor,
                idGenerator,
                messageSender,
                modelWriter,
                lookupEntryStore,
                contentResolver,
                contentWriter,
                equivalenceBreaker,
                oldContentDeactivator
        );
    }

    @Test
    public void unpublishContentById() throws Exception {
        when(request.getParameter(id)).thenReturn("h");
        when(substitutionTableNumberCodec.decode(anyString())).thenReturn(BigInteger.ZERO);
        when(entryStoreIterator.hasNext()).thenReturn(true);
        when(entryStoreIterator.next()).thenReturn(lookupEntry);

        unpublishContent();
    }

    @Test
    public void unpublishContentByUri() throws Exception {
        when(request.getParameter(id)).thenReturn(null);
        when(request.getParameter(uri)).thenReturn(uri);
        when(entryStoreIterator.hasNext()).thenReturn(true);
        when(entryStoreIterator.next()).thenReturn(lookupEntry);

        when(lookupEntryStore
                .entriesForCanonicalUris(Lists.newArrayList(uri))).thenReturn(lookupEntries);
        when(lookupEntries.iterator()).thenReturn(entryStoreIterator);

        unpublishContent();
    }

    public void unpublishContent() {
        described.setActivelyPublished(true);
        when(resolvedContent.isEmpty()).thenReturn(false);
        when(resolvedContent.getFirstValue()).thenReturn(identifiedMaybe);
        when(identifiedMaybe.hasValue()).thenReturn(true);
        when(identifiedMaybe.requireValue()).thenReturn(described);
        when(described.getPublisher()).thenReturn(Publisher.METABROADCAST);

        controller.unpublishContent(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertThat(described.isActivelyPublished(), is(false));
    }

    @Test
    public void postSynchronously() throws Exception {
        when(request.getParameter(ContentWriteController.ASYNC_PARAMETER))
                .thenReturn("false");

        String assertionParameterValue =
                "\"channelUri\"|\"2016-01-01T00:00:00Z\"|\"2016-01-02T00:00:00Z\"";
        when(request.getParameter(ContentWriteController.BROADCAST_ASSERTIONS_PARAMETER))
                .thenReturn(assertionParameterValue);

        BroadcastMerger expectedMerger = BroadcastMerger.parse(assertionParameterValue);

        controller.postContent(request, response);

        verify(writeExecutor).parseInputStream(streamCaptor.capture(), anyBoolean());
        assertThat(IOUtils.toByteArray(streamCaptor.getValue()), is(inputBytes));

        verify(writeExecutor).writeContent(
                contentCaptor.capture(),
                eq(inputContent.getType()),
                eq(true),
                eq(expectedMerger)
        );

        verify(messageSender, never()).sendMessage(
                any(ContentWriteMessage.class), any()
        );

        Content actualContent = contentCaptor.getValue();
        assertThat(actualContent.getId(), is(contentId));

        inputContent.getContent().setId(contentId);
        assertThat(actualContent, is(inputContent.getContent()));

        verify(response).setStatus(HttpStatus.OK.value());
        verify(response).setHeader(eq(HttpHeaders.LOCATION), locationCaptor.capture());

        String expectedEncodedId = codec.encode(BigInteger.valueOf(contentId));
        assertThat(locationCaptor.getValue().contains("/3.0/content.json?id=" + expectedEncodedId),
                is(true));
    }

    @Test
    public void postAsynchronously() throws Exception {
        when(request.getParameter(ContentWriteController.ASYNC_PARAMETER))
                .thenReturn("true");

        controller.postContent(request, response);

        verify(writeExecutor).parseInputStream(streamCaptor.capture(), anyBoolean());
        assertThat(IOUtils.toByteArray(streamCaptor.getValue()), is(inputBytes));

        verify(writeExecutor, never()).writeContent(any(Content.class), anyString(), anyBoolean());

        verify(messageSender).sendMessage(
                messageCaptor.capture(), eq(String.valueOf(contentId).getBytes())
        );

        ContentWriteMessage actualMessage = messageCaptor.getValue();
        assertThat(actualMessage.getMessageId(), is(not(nullValue())));
        assertThat(actualMessage.getTimestamp(), is(not(nullValue())));
        assertThat(actualMessage.getContentId(), is(contentId));
        assertThat(actualMessage.getShouldMerge(), is(true));
        assertThat(actualMessage.getContentBytes(), is(inputBytes));

        verify(response).setStatus(HttpStatus.ACCEPTED.value());
        verify(response).setHeader(eq(HttpHeaders.LOCATION), locationCaptor.capture());

        String expectedEncodedId = codec.encode(BigInteger.valueOf(contentId));
        assertThat(locationCaptor.getValue().contains("/3.0/content.json?id=" + expectedEncodedId),
                is(true));
    }

    @Test
    public void deactivateContentWithCorrectParams() throws Exception {

        when(request.getParameter("source")).thenReturn("metabroadcast.com");
        when(request.getParameter("valid_uris")).thenReturn("uri1,uri2,uri3");

        controller.unpublishContent(request, response);

        verify(oldContentDeactivator, times(1)).deactivateOldContent(
                Publisher.METABROADCAST,
                ImmutableList.of("uri1","uri2","uri3"),
                50);
    }

    @Test
    public void doNotDeactivateWithoutWritePermissions() throws Exception {

        when(request.getParameter("source")).thenReturn("upcoming.radiotimes.com");
        when(request.getParameter("valid_uris")).thenReturn("uri1,uri2,uri3");

        controller.unpublishContent(request, response);

        ArgumentCaptor<AtlasErrorSummary> argument = ArgumentCaptor.forClass(
                AtlasErrorSummary.class
        );

        verify(modelWriter).writeError(eq(request), eq(response), argument.capture());

        assertThat("403", is(argument.getValue().statusCode().toString()));
        assertThat("API key does not have write permission",
                is(argument.getValue().message())
        );
    }
}
