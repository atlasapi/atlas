package org.atlasapi.remotesite.knowledgemotion;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ingest.s3.process.FileProcessor;
import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionFileProcessor implements FileProcessor {

    private static final ImmutableList<KnowledgeMotionSourceConfig> SOURCES = ImmutableList.of(
        KnowledgeMotionSourceConfig.from("GlobalImageworks", Publisher.KM_GLOBALIMAGEWORKS, "globalImageWorks:%s", "http://globalimageworks.com/%s"),
        KnowledgeMotionSourceConfig.from("BBC Worldwide", Publisher.KM_BBC_WORLDWIDE, "km-bbcWorldwide:%s", "http://bbc.knowledgemotion.com/%s"),
        KnowledgeMotionSourceConfig.from("British Movietone", Publisher.KM_MOVIETONE, "km-movietone:%s", "http://movietone.knowledgemotion.com/%s"),
        KnowledgeMotionSourceConfig.from("Bloomberg", Publisher.KM_BLOOMBERG, "bloomberg:%s", "http://bloomberg.com/%s")
    );

    private static final ImmutableList<KnowledgeMotionSourceConfig> FIX_SOURCES = ImmutableList.of(
        KnowledgeMotionSourceConfig.from("Bloomberg", Publisher.KM_BLOOMBERG, "bloomberg:%s", "http://bloomberg.com/%s")
    );

    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final ContentLister contentLister;
    private final TopicGuesser topicGuesser;
    private final KnowledgeMotionCsvTranslator csvTranslator;

    public KnowledgeMotionFileProcessor(ContentResolver contentResolver, ContentWriter contentWriter,
        ContentLister contentLister, TopicGuesser topicGuesser) {
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.contentLister = contentLister;
        this.topicGuesser = topicGuesser;

        csvTranslator = new KnowledgeMotionCsvTranslator();
    }

    @Override
    public ProcessingResult process(String originalFilename, File file) {
        ProcessingResult processingResult = new ProcessingResult();

        List<KnowledgeMotionDataRow> rows;
        try {
            rows = csvTranslator.translate(file);
        } catch (IOException e) {
            processingResult.error("input file", "Unable to parse input file: " + e.getMessage());
            return processingResult;
        }

        KnowledgeMotionUpdater updater = new KnowledgeMotionUpdater(SOURCES,
            new KnowledgeMotionContentMerger(contentResolver, contentWriter,
                new KnowledgeMotionDataRowContentExtractor(SOURCES, topicGuesser)), contentLister);
        processingResult = updater.process(rows, processingResult);

        KnowledgeMotionSpecialIdFixer specialIdFixer = new KnowledgeMotionSpecialIdFixer(new SpecialIdFixingKnowledgeMotionDataRowHandler(contentResolver, contentWriter, FIX_SOURCES.get(0)));
        specialIdFixer.process(rows, processingResult);

        return processingResult;
    }

}
