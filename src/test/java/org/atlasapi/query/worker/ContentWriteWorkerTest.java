package org.atlasapi.query.worker;

import java.io.ByteArrayInputStream;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.content.ContentWriteExecutor;

import com.metabroadcast.common.time.Timestamp;

import com.amazonaws.util.IOUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentWriteWorkerTest {

    @Captor
    private ArgumentCaptor<ByteArrayInputStream> inputStreamCaptor;

    @Captor
    private ArgumentCaptor<Content> contentCaptor;

    private ContentWriteWorker worker;
    private ContentWriteMessage message;

    private @Mock ContentWriteExecutor writeExecutor;

    private byte[] messageBytes;
    private ContentWriteExecutor.InputContent inputContent;

    @Before
    public void setUp() throws Exception {
        messageBytes = "{ content: {} }".getBytes();
        inputContent = new ContentWriteExecutor.InputContent(
                new Item("uri", "curie", Publisher.METABROADCAST), "item"
        );
        when(writeExecutor.parseInputStream(any(ByteArrayInputStream.class)))
                .thenReturn(inputContent);

        worker = new ContentWriteWorker(writeExecutor);
        message = new ContentWriteMessage(
                "id", Timestamp.of(DateTime.now()), messageBytes, 0L, true
        );
    }

    @Test
    public void processCallsWriteExecutor() throws Exception {
        worker.process(message);

        verify(writeExecutor).parseInputStream(inputStreamCaptor.capture());

        byte[] actualBytes = IOUtils.toByteArray(inputStreamCaptor.getValue());
        assertThat(actualBytes, is(messageBytes));

        verify(writeExecutor).writeContent(
                contentCaptor.capture(), eq(inputContent.getType()), eq(message.getShouldMerge())
        );

        Content actualContent = contentCaptor.getValue();
        assertThat(actualContent.getId(), is(message.getContentId()));
        assertThat(actualContent.getCanonicalUri(), is(inputContent.getContent().getCanonicalUri()));
    }
}
