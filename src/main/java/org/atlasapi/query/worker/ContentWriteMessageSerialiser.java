package org.atlasapi.query.worker;

import java.io.IOException;

import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.serialization.json.JsonFactory;

import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.queue.MessageSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

public class ContentWriteMessageSerialiser implements MessageSerializer<ContentWriteMessage> {

    private final ObjectMapper mapper;

    public ContentWriteMessageSerialiser() {
        mapper = JsonFactory.makeJsonMapper();
        mapper.registerModule(new JacksonMessageSerializer.MessagingModule());
        mapper.addMixInAnnotations(
                ContentWriteMessage.class, ContentWriteMessageConfiguration.class
        );
    }

    @Override
    public final byte[] serialize(ContentWriteMessage msg) {
        try {
            return mapper.writeValueAsBytes(msg);
        } catch (IOException e) {
            throw Throwables.propagate(new MessageSerializationException(e));
        }
    }

    @Override
    public final ContentWriteMessage deserialize(byte[] bytes) {
        try {
            return mapper.readValue(bytes, ContentWriteMessage.class);
        } catch (Exception e) {
            throw Throwables.propagate(new MessageDeserializationException(e));
        }
    }
}
