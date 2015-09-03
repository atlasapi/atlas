package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionSpecialIdFixer {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionSpecialIdFixer.class);

    private final KnowledgeMotionDataRowHandler dataHandler;

    public KnowledgeMotionSpecialIdFixer(SpecialIdFixingKnowledgeMotionDataRowHandler dataHandler) {
        this.dataHandler = checkNotNull(dataHandler);
    }

    protected ProcessingResult.Builder process(Iterator<KnowledgeMotionDataRow> rows,
            ProcessingResult.Builder resultBuilder) {
        if (!rows.hasNext()) {
            log.info("Knowledgemotion Common Ingest received an empty file");
            resultBuilder.error("input file", "Empty file");
        }

        while (rows.hasNext()) {
            KnowledgeMotionDataRow row = rows.next();
            try {
                dataHandler.handle(row);
                log.debug("Successfully fixed special ID for row {}", row.getId());
                // these lines cause double reporting for rows
//                processingResult.success();
            } catch (RuntimeException e) {
                log.debug("Failed to fix special ID for row {}", row.getId(), e);
//                processingResult.error(row.getId(), "While fixing special ID: " + e.getMessage());
            }
        }

        return resultBuilder;
    }

}
