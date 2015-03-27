package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionSpecialIdFixer {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionSpecialIdFixer.class);

    private final KnowledgeMotionDataRowHandler dataHandler;

    public KnowledgeMotionSpecialIdFixer(SpecialIdFixingKnowledgeMotionDataRowHandler dataHandler) {
        this.dataHandler = checkNotNull(dataHandler);
    }

    protected ProcessingResult process(List<KnowledgeMotionDataRow> rows, ProcessingResult processingResult) {
        for (KnowledgeMotionDataRow row : rows) {
            log.info("Processing row {}", row.getId());
            try {
                dataHandler.handle(row);
                log.debug("Successfully fixed special ID for row {}", row.getId());
                processingResult.success();
            } catch (RuntimeException e) {
                log.debug("Failed to fix special ID for row {}", row.getId());
                processingResult.error(row.getId(), "While fixing special ID: " + e.getMessage());
            }
        }

        return processingResult;
    }

}
