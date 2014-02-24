package org.atlasapi.remotesite.rovi.program;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.rovi.KeyedFileIndex;
import org.atlasapi.remotesite.rovi.RoviContentExtractor;
import org.atlasapi.remotesite.rovi.RoviShowType;
import org.atlasapi.remotesite.rovi.series.RoviEpisodeSequenceLine;
import org.atlasapi.remotesite.rovi.series.RoviSeasonHistoryLine;
import org.atlasapi.remotesite.rovi.series.RoviSeriesLine;

public class ProgramLineContentExtractorSupplier {

    private final KeyedFileIndex<String, RoviProgramDescriptionLine> descriptionIndex;
    private final KeyedFileIndex<String, RoviSeriesLine> seriesIndex;
    private final KeyedFileIndex<String, RoviEpisodeSequenceLine> episodeSequenceIndex;
    private final KeyedFileIndex<String, RoviSeasonHistoryLine> seasonHistoryIndex;
    private final ContentResolver contentResolver;

    public ProgramLineContentExtractorSupplier(
            KeyedFileIndex<String, RoviProgramDescriptionLine> descriptionIndex,
            KeyedFileIndex<String, RoviSeriesLine> seriesIndex,
            KeyedFileIndex<String, RoviEpisodeSequenceLine> episodeSequenceIndex,
            KeyedFileIndex<String, RoviSeasonHistoryLine> seasonHistoryIndex,
            ContentResolver contentResolver) {
        this.descriptionIndex = descriptionIndex;
        this.seriesIndex = seriesIndex;
        this.episodeSequenceIndex = episodeSequenceIndex;
        this.contentResolver = contentResolver;
        this.seasonHistoryIndex = seasonHistoryIndex;
    }

    public RoviContentExtractor<RoviProgramLine, ? extends Content> getContentExtractor(
            RoviShowType programType) {

        switch (programType) {
        case MO:
            return new ProgramLineFilmExtractor(descriptionIndex, contentResolver);
        case SE:
            return new ProgramLineEpisodeExtractor(descriptionIndex, episodeSequenceIndex, seasonHistoryIndex, contentResolver);
        case SM:
            return new ProgramLineBrandExtractor(descriptionIndex, seriesIndex, contentResolver);
        case OT:
            return new ProgramLineItemExtractor(descriptionIndex, contentResolver);
        default:
            throw new RuntimeException("Program type not supported");
        }

    }

}