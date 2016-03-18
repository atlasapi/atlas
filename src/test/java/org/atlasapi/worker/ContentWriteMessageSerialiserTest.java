package org.atlasapi.worker;

import java.util.UUID;

import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.query.worker.ContentWriteMessageSerialiser;

import com.metabroadcast.common.time.Timestamp;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContentWriteMessageSerialiserTest {

    private ContentWriteMessage message;
    private ContentWriteMessageSerialiser serialiser;

    @Before
    public void setUp() throws Exception {
        message = new ContentWriteMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(DateTime.now()),
                "{ content: { foo: bar } }".getBytes(),
                0L,
                true
        );
        serialiser = new ContentWriteMessageSerialiser();
    }

    @Test
    public void testSerialisationDeserialisation() throws Exception {
        byte[] serialisedBytes = serialiser.serialize(message);
        ContentWriteMessage deserialisedMessage = serialiser.deserialize(serialisedBytes);

        assertThat(deserialisedMessage.getMessageId(), is(message.getMessageId()));
        assertThat(deserialisedMessage.getTimestamp(), is(message.getTimestamp()));
        assertThat(deserialisedMessage.getContentBytes(), is(message.getContentBytes()));
        assertThat(deserialisedMessage.getContentId(), is(message.getContentId()));
        assertThat(deserialisedMessage.getShouldMerge(), is(message.getShouldMerge()));
    }
}
