package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.btvod.BtVodProductType.EPISODE;
import static org.atlasapi.remotesite.btvod.BtVodProductType.HELP;
import static org.atlasapi.remotesite.btvod.BtVodProductType.SEASON;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


public class BrandUriExtractor {

    private static final Pattern HD_PATTERN = Pattern.compile("^(.*)\\-\\sHD");

    private static final List<Pattern> BRAND_TITLE_FROM_EPISODE_PATTERNS = ImmutableList.of(
            Pattern.compile("^(.*):.*S[0-9]+.*S[0-9]+\\-E.*"),  
            Pattern.compile("^(.*).*S[0-9]+.*S[0-9]+\\-E.*"),  
            Pattern.compile("^(.*?)-\\s+.*S[0-9]++[\\- ]E.*"),
            Pattern.compile("^(.*).*S[0-9]++[\\- ]E.*"),
            Pattern.compile("^(.*)Season\\s[0-9]+\\s-\\sSeason\\s[0-9]+\\sEpisode\\s[0-9]+.*"),
            Pattern.compile("^(.*),\\s+Series\\s+([0-9])+,.*"),
            Pattern.compile("^(.*?)\\s+\\-\\s+.*")
    );

    private static final List<Pattern> BRAND_TITLE_FROM_SERIES_PATTERNS = ImmutableList.of(
            Pattern.compile("^(.*?)-\\s+.*Series.*"),
            Pattern.compile("^(.*) Series [0-9]+"),
            Pattern.compile("^(.*) S[0-9]+")
    );


    private final String uriPrefix;
    private final TitleSanitiser titleSanitiser;
    
    public BrandUriExtractor(String uriPrefix, TitleSanitiser titleSanitiser) {
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.uriPrefix = checkNotNull(uriPrefix);    
    }
    
    public Optional<String> extractBrandUri(BtVodEntry entry) {
        if (!shouldSynthesizeBrand(entry)) {
            return Optional.absent();
        } else {
            return Optional.of(uriPrefix + "synthesized/brands/" + ensureSynthesizedKey(entry));
        }
    }
    
    private String ensureSynthesizedKey(BtVodEntry row) {
        Optional<String> synthesizedKey = getSynthesizedKey(row);

        if (!synthesizedKey.isPresent()) {
            String productId = row.getGuid();
            throw new RuntimeException("Not able to generate a synthesized key for a synthesizable brand from row: [PRODUCT_ID="+productId+"]");
        }

        return synthesizedKey.get();
    }
    
    public Optional<String> getSynthesizedKey(BtVodEntry row) {
        String title = row.getTitle();

        if (EPISODE.isOfType(row.getProductType()) && canParseBrandFromEpisode(row)) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromEpisodeTitle(title)));
        }
        if (SEASON.isOfType(row.getProductType())) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromSeriesTitle(title)));
        }

        return Optional.absent();
    }
    
    public String brandTitleFromEpisodeTitle(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            Matcher matcher = brandPattern.matcher(stripHDSuffix(title));
            if (matcher.matches()) {
                return titleSanitiser.sanitiseTitle(matcher.group(1));
            }

        }
        return null;
    }
    public String brandTitleFromSeriesTitle(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_SERIES_PATTERNS) {
            Matcher matcher = brandPattern.matcher(stripHDSuffix(title));
            if (matcher.matches()) {
                return titleSanitiser.sanitiseTitle(matcher.group(1));
            }

        }
        return stripHDSuffix(title);
    }

    public boolean shouldSynthesizeBrand(BtVodEntry row) {
        return !HELP.isOfType(row.getProductType())
                && ((EPISODE.isOfType(row.getProductType()) && canParseBrandFromEpisode(row))
                || (SEASON.isOfType(row.getProductType())));
    }

    public boolean canParseBrandFromEpisode(BtVodEntry row) {
        return isTitleSyntesizableFromEpisode(row.getTitle());
    }

    private boolean isTitleSyntesizableFromEpisode(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            if (brandPattern.matcher(stripHDSuffix(title)).matches()) {
                return true;
            }
        }
        return false;
    }

    public boolean canParseBrandFromSeries(BtVodEntry row) {
        return isTitleSyntesizableFromSeries(row.getTitle());
    }

    private boolean isTitleSyntesizableFromSeries(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_SERIES_PATTERNS) {
            if (brandPattern.matcher(stripHDSuffix(title)).matches()) {
                return true;
            }
        }
        return false;
    }


    private String stripHDSuffix(String title) {
        Matcher hdMatcher = HD_PATTERN.matcher(title);
        if (hdMatcher.matches()) {
            return hdMatcher.group(1).trim().replace("- HD ", "");
        }
        return title;
    }

}
