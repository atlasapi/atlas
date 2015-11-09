package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodSeriesUriExtractor {

    private static final String SERIES_TYPE = "season";
    private static final List<Pattern> SERIES_NUMBER_PATTERNS = ImmutableList.of(
            Pattern.compile("S([0-9]+)[\\- ]E[0-9]+"),
            Pattern.compile("^.*Season\\s([0-9]+)\\s-\\sSeason\\s[0-9]+\\sEpisode\\s[0-9]+.*"),
            Pattern.compile("^.*Series\\s+([0-9])+.*")
    );

    private final BrandUriExtractor brandUriExtractor;

    public BtVodSeriesUriExtractor(BrandUriExtractor brandUriExtractor) {
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
    }

    public Optional<String> seriesUriFor(BtVodEntry row) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(row);
        if(!brandUri.isPresent()) {
            return Optional.absent();
        }
        Optional<Integer> seriesNumber = extractSeriesNumber(row);
        if (!seriesNumber.isPresent()) {
            return Optional.absent();
        }

        return Optional.of(brandUri.get() + "/series/" + seriesNumber.get());

    }

    protected Optional<Integer> extractSeriesNumber(BtVodEntry row) {
        if (SERIES_TYPE.equals(row.getProductType())) {
            return Optional.fromNullable(row.getSeriesNumber());
        }
        String title = row.getTitle();
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
}
