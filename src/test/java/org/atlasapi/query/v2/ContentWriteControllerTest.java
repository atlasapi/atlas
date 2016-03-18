package org.atlasapi.query.v2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.worker.ContentWriteMessage;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;

import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.DelegatingServletInputStream;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentWriteControllerTest {

    private ContentWriteController controller;

    private @Captor ArgumentCaptor<Content> contentCaptor;
    private @Captor ArgumentCaptor<String> locationCaptor;
    private @Captor ArgumentCaptor<ContentWriteMessage> messageCaptor;
    private @Captor ArgumentCaptor<InputStream> streamCaptor;

    private @Mock ApplicationConfigurationFetcher configurationFetcher;
    private @Mock ContentWriteExecutor writeExecutor;
    private @Mock LookupBackedContentIdGenerator idGenerator;
    private @Mock MessageSender<ContentWriteMessage> messageSender;

    private @Mock HttpServletRequest request;
    private @Mock HttpServletResponse response;

    private NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private byte[] inputBytes;
    private ContentWriteExecutor.InputContent inputContent;
    private long contentId;

    @Before
    public void setUp() throws Exception {
        ApplicationConfiguration configuration = ApplicationConfiguration.defaultConfiguration()
                .withSource(Publisher.METABROADCAST, SourceStatus.AVAILABLE_ENABLED)
                .copyWithWritableSources(ImmutableList.of(Publisher.METABROADCAST));

        inputBytes = "{ content: {} }".getBytes();
        DelegatingServletInputStream inputStream = new DelegatingServletInputStream(
                new ByteArrayInputStream(inputBytes)
        );
        Item content = new Item();
        content.setCanonicalUri("uri");
        content.setPublisher(Publisher.METABROADCAST);
        inputContent = new ContentWriteExecutor.InputContent(content, "item");
        contentId = 0L;

        when(configurationFetcher.configurationFor(request)).thenReturn(Maybe.just(configuration));
        when(request.getInputStream()).thenReturn(inputStream);
        when(writeExecutor.parseInputStream(any(InputStream.class))).thenReturn(inputContent);
        when(idGenerator.getId(any(Content.class))).thenReturn(contentId);

        controller = new ContentWriteController(
                configurationFetcher, writeExecutor, idGenerator, messageSender
        );
    }

    @Test
    public void postSynchronously() throws Exception {
        when(request.getParameter(ContentWriteController.ASYNC_PARAMETER))
                .thenReturn("false");

        controller.postContent(request, response);

        verify(writeExecutor).parseInputStream(streamCaptor.capture());
        assertThat(IOUtils.toByteArray(streamCaptor.getValue()), is(inputBytes));

        verify(writeExecutor).writeContent(
                contentCaptor.capture(), eq(inputContent.getType()), eq(true)
        );

        verify(messageSender, never()).sendMessage(
                any(ContentWriteMessage.class), (byte[]) any()
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

        verify(writeExecutor).parseInputStream(streamCaptor.capture());
        assertThat(IOUtils.toByteArray(streamCaptor.getValue()), is(inputBytes));

        verify(writeExecutor, never()).writeContent(any(Content.class), anyString(), anyBoolean());

        verify(messageSender).sendMessage(
                messageCaptor.capture(), eq(String.valueOf(contentId).getBytes())
        );

        ContentWriteMessage actualMessage = messageCaptor.getValue();
        assertThat(actualMessage.getMessageId(), is(not(nullValue())));
        assertThat(actualMessage.getTimestamp(), is(not(nullValue())));
        assertThat(actualMessage.getContentid(), is(contentId));
        assertThat(actualMessage.getShouldMerge(), is(true));
        assertThat(actualMessage.getContentBytes(), is(inputBytes));

        verify(response).setStatus(HttpStatus.ACCEPTED.value());
        verify(response).setHeader(eq(HttpHeaders.LOCATION), locationCaptor.capture());

        String expectedEncodedId = codec.encode(BigInteger.valueOf(contentId));
        assertThat(locationCaptor.getValue().contains("/3.0/content.json?id=" + expectedEncodedId),
                is(true));
    }
}
