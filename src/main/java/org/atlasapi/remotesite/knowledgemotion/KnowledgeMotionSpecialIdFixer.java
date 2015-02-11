package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionSpecialIdFixer {

    private final KnowledgeMotionDataRowHandler dataHandler;

    public KnowledgeMotionSpecialIdFixer(SpecialIdFixingKnowledgeMotionDataRowHandler dataHandler) {
        this.dataHandler = checkNotNull(dataHandler);
    }

    protected ProcessingResult process(List<KnowledgeMotionDataRow> rows, ProcessingResult processingResult) {
        for (KnowledgeMotionDataRow row : rows) {
            try {
                dataHandler.handle(row);
            } catch (RuntimeException e) {
                processingResult.error(row.getId(), e.getMessage());
            }
        }

        return processingResult;
    }

}
