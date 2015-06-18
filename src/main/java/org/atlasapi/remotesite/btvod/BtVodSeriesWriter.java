package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodSeriesWriter implements BtVodDataProcessor<UpdateProgress>{

    private static final Logger log = LoggerFactory.getLogger(BtVodSeriesWriter.class);
    private static final List<Pattern> SERIES_NUMBER_PATTERNS = ImmutableList.of(
            Pattern.compile("S([0-9]+)\\-E[0-9]+"),
            Pattern.compile("^.*Season\\s([0-9]+)\\s-\\sSeason\\s[0-9]+\\sEpisode\\s[0-9]+.*")
    );
    private static final String HELP_TYPE = "help";
    private static final String EPISODE_TYPE = "episode";


    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandWriter brandExtractor;
    private final Publisher publisher;
    private final ContentMerger contentMerger;
    private final Map<String, ParentRef> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private UpdateProgress progress = UpdateProgress.START;


    public BtVodSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows
    ) {
        this.processedRows = checkNotNull(processedRows);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.publisher = checkNotNull(publisher);
        //TODO: Use DescribedFieldsExtractor for all described fields, not just aliases.
        //      Added as a collaborator for Alias extraction, but should be used more 
        //      widely
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!isPartOfSeries(row)
                    || processedRows.contains(uriFor(row).get())) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Series series = seriesFrom(row);
            write(series);

            // This allows a lookup by series title. Note that the only reference from an episode to a series is the series title.
            // Consequently this map will be used to lookup SeriesRef when processing episodes
            // TODO: is there a better approach than this ^?
            processedSeries.put(uriFor(row).get(), ParentRef.parentRefFrom(series));
            listener.onContent(series, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
    }

    private boolean isPartOfSeries(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getProductType())
                && EPISODE_TYPE.equals(row.getProductType())
                && extractSeriesNumber(row.getTitle()).isPresent();
    }

    private void write(Series extracted) {
        Maybe<Identified> existing = resolver
                .findByCanonicalUris(ImmutableSet.of(extracted.getCanonicalUri()))
                .getFirstValue();
        
        if (existing.hasValue()) {
            Container merged = contentMerger.merge((Series) existing.requireValue(), 
                                                   extracted);
            writer.createOrUpdate(merged);
        } else {
            writer.createOrUpdate(extracted);
        }
    }

    private Series seriesFrom(BtVodEntry row) {
        Series series = new Series(uriFor(row).get(), null, publisher);
        //TODO more fields
        series.withSeriesNumber(extractSeriesNumber(row.getTitle()).get());
        series.setParentRef(brandExtractor.getBrandRefFor(row).orNull());
        series.setAliases(describedFieldsExtractor.aliasesFrom(row));
        series.setGenres(describedFieldsExtractor.btGenresFrom(row));
        return series;
    }

    public Optional<Integer> extractSeriesNumber(String title) {
        if (title == null) {
            return Optional.absent();
        }

        for (Pattern seriesNumberPattern : SERIES_NUMBER_PATTERNS) {
            Matcher matcher = seriesNumberPattern.matcher(title);
            if (matcher.find()) {
                return Optional.of(Ints.tryParse(matcher.group(1)));
            }

        }



        return Optional.absent();
    }

    public Optional<String> uriFor(BtVodEntry row) {
        Optional<String> brandUri = brandExtractor.uriFor(row);
        if(!brandUri.isPresent()) {
            return Optional.absent();
        }
        Optional<Integer> seriesNumber = extractSeriesNumber(row.getTitle());
        if (!seriesNumber.isPresent()) {
            return Optional.absent();
        }

        return Optional.of(brandUri.get() +  "/series/" + seriesNumber.get());
    }
    
    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getSeriesRefFor(BtVodEntry row) {
        Optional<String> seriesUri = uriFor(row);
        if(seriesUri.isPresent()) {
            return Optional.fromNullable(processedSeries.get(seriesUri.get()));
        }
        return Optional.absent();
    }

}
