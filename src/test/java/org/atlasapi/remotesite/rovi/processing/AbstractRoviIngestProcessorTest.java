package org.atlasapi.remotesite.rovi.processing;

import static org.atlasapi.remotesite.rovi.RoviTestUtils.resolvedContent;
import static org.atlasapi.remotesite.rovi.RoviTestUtils.unresolvedContent;

import java.util.Map;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.rovi.RoviConstants;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.indexing.MapBasedKeyedFileIndexer;
import org.atlasapi.remotesite.rovi.model.RoviEpisodeSequenceLine;
import org.atlasapi.remotesite.rovi.model.RoviProgramDescriptionLine;
import org.atlasapi.remotesite.rovi.parsers.RoviEpisodeSequenceLineParser;
import org.atlasapi.remotesite.rovi.parsers.RoviProgramDescriptionLineParser;
import org.atlasapi.remotesite.rovi.processing.restartable.IngestStatusStore;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

public class AbstractRoviIngestProcessorTest {

    protected MapBackedContentWriter contentWriter = new MapBackedContentWriter();
    protected RoviContentWriter roviContentWriter = new RoviContentWriter(contentWriter);

    @Mock
    protected ContentResolver contentResolver;

    @Mock
    protected ScheduleFileProcessor scheduleProcessor;

    @Mock
    protected IngestStatusStore statusStore;

    protected MapBasedKeyedFileIndexer<String, RoviProgramDescriptionLine> descriptionsIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviProgramDescriptionLineParser());
    }

    protected MapBasedKeyedFileIndexer<String, RoviEpisodeSequenceLine> episodeSequenceIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviEpisodeSequenceLineParser());
    }

    protected Answer<ResolvedContent> writtenOrUnresolved(final String canonicalUri) {
        return new Answer<ResolvedContent>() {
            @Override public ResolvedContent answer(InvocationOnMock invocationOnMock)
                    throws Throwable {
                return writtenContentOrUnresolved(canonicalUri);
            }
        };
    }

    protected ResolvedContent writtenContentOrUnresolved(String canonicalUri) {
        Optional<Content> writtenItem = contentWriter.getWrittenItem(canonicalUri);

        if (writtenItem.isPresent()) {
            return resolvedContent(writtenItem.get());
        }

        return unresolvedContent();
    }

    protected static class MapBackedContentWriter implements ContentWriter {

        private final Map<String, Content> contents = Maps.newHashMap();

        public Map<String, Content> getItems() {
            return contents;
        }

        public Optional<Content> getWrittenItem(String canonicalUri) {
            return Optional.fromNullable(contents.get(canonicalUri));
        }

        public boolean hasWritten() {
            return !contents.isEmpty();
        }

        @Override
        public Item createOrUpdate(Item item) {
            contents.put(item.getCanonicalUri(), item);
            return item;
        }

        @Override
        public void createOrUpdate(Container container) {
            contents.put(container.getCanonicalUri(), container);
        }
    }

}
