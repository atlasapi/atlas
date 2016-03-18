package org.atlasapi.query.worker;

import java.io.ByteArrayInputStream;

import org.atlasapi.media.entity.Content;
import org.atlasapi.query.content.ContentWriteExecutor;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteWorker implements Worker<ContentWriteMessage> {

    private static final Logger log = LoggerFactory.getLogger(ContentWriteWorker.class);

    private final ContentWriteExecutor writeExecutor;

    public ContentWriteWorker(ContentWriteExecutor writeExecutor) {
        this.writeExecutor = checkNotNull(writeExecutor);
    }

    @Override
    public void process(ContentWriteMessage message) throws RecoverableException {
        try {
            log.debug("Processing message on {}", message.getContentid());

            ContentWriteExecutor.InputContent inputContent = writeExecutor.parseInputStream(
                    new ByteArrayInputStream(message.getContentBytes())
            );
            Content content = inputContent.getContent();
            content.setId(message.getContentid());
            writeExecutor.writeContent(content, inputContent.getType(), message.getShouldMerge());
        } catch (Exception e) {
            log.error("Failed to write content", e);
            throw Throwables.propagate(e);
        }
    }
}
