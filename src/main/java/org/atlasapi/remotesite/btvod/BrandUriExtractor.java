package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;


public class BrandUriExtractor {

    private static final String HELP_TYPE = "help";
    private static final String EPISODE_TYPE = "episode";
    
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

        if (canParseBrandFromEpisode(row)) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromEpisodeTitle(title)));
        }

        return Optional.absent();
    }
    
    private String brandTitleFromEpisodeTitle(String title) {
        for (Pattern brandPattern : BtVodBrandWriter.BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            Matcher matcher = brandPattern.matcher(stripHDSuffix(title));
            if (matcher.matches()) {
                return titleSanitiser.sanitiseTitle(matcher.group(1));
            }

        }
        return null;
    }

    private boolean shouldSynthesizeBrand(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getProductType())
                && EPISODE_TYPE.equals(row.getProductType())
                && canParseBrandFromEpisode(row);
    }

    private boolean canParseBrandFromEpisode(BtVodEntry row) {
        return isTitleSyntesizableFromEpisode(row.getTitle());
    }

    private boolean isTitleSyntesizableFromEpisode(String title) {
        for (Pattern brandPattern : BtVodBrandWriter.BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            if (brandPattern.matcher(stripHDSuffix(title)).matches()) {
                return true;
            }

        }
        return false;
    }
    
    private String stripHDSuffix(String title) {
        Matcher hdMatcher = BtVodBrandWriter.HD_PATTERN.matcher(title);
        if (hdMatcher.matches()) {
            return hdMatcher.group(1).trim().replace("- HD ", "");
        }
        return title;
    }

}
