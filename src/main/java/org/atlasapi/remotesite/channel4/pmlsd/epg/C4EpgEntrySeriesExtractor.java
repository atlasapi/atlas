package org.atlasapi.remotesite.channel4.pmlsd.epg;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.channel4.pmlsd.ContentFactory;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;

import javax.annotation.Nullable;

public class C4EpgEntrySeriesExtractor implements ContentExtractor<C4EpgEntry, Optional<Series>> {

    private final ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory;
    
    public C4EpgEntrySeriesExtractor(ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory) {
        this.contentFactory = contentFactory;
    }
    
    @Override
    public Optional<Series> extract(C4EpgEntry source) {
        Optional<Series> possibleSeries = contentFactory.createSeries(source);
        
        if (possibleSeries.isPresent()) {
            Series series = possibleSeries.get();
            //TODO set alias URI based on publisher
            series.addAliasUrl(C4AtomApi.hierarchyUriFromCanonical(series.getCanonicalUri()));
            
            if (source.seriesNumber() != null) {
                series.withSeriesNumber(source.seriesNumber());
            } else {
                series.withSeriesNumber(extractSeriesNumberFrom(series.getCanonicalUri()));
            }
            return Optional.of(series);
        }
        return Optional.empty();
    }

    @Nullable private Integer extractSeriesNumberFrom(String seriesUri) {
        Pattern pattern = Pattern.compile("(\\d+)$");
        Matcher matcher = pattern.matcher(seriesUri);
        if (matcher.matches()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

}
