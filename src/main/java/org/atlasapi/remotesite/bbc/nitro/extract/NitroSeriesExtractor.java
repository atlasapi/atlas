package org.atlasapi.remotesite.bbc.nitro.extract;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Series;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link NitroContentExtractor} for extracting
 * {org.atlasapi.media.entity.Series Atlas Series} from {@link Series Nitro
 * Series}.
 */
public class NitroSeriesExtractor
        extends NitroContentExtractor<Series, org.atlasapi.media.entity.Series> {

    public NitroSeriesExtractor(Clock clock) {
        super(clock);
    }

    @Override
    protected org.atlasapi.media.entity.Series createContent(Series source) {
        return new org.atlasapi.media.entity.Series();
    }

    @Override
    protected String extractPid(Series source) {
        return source.getPid();
    }

    @Override
    protected String extractTitle(Series source) {
        return source.getTitle();
    }

    @Override
    protected Synopses extractSynopses(Series source) {
        return source.getSynopses();
    }

    @Override protected Brand.Contributions extractContributions(Series series) {
        return series.getContributions();
    }

    @Override
    protected Brand.Images.Image extractImage(Series source) {
        if (source.getImages() == null) {
            return null;
        }
        return source.getImages().getImage();
    }

    @Nullable
    @Override
    protected AvailableVersions extractVersions(Series series) {
        return null;
    }

    @Nullable
    @Override
    protected Integer extractReleaseYear(Series source) {
        if (source.getReleaseYear() != null) {
            return source.getReleaseYear().getYear();
        } else if (source.getReleaseDate() != null) {
            return source.getReleaseDate().getYear();
        } else {
            return null;
        }
    }

    @Override
    protected void extractAdditionalFields(Series source, org.atlasapi.media.entity.Series content,
            DateTime now) {
        //Dont use source.getSeriesOf().getPosition(), as per ENG-508
        Integer seriesNum = getSeriesNumberFromTitle(source.getTitle());
        if (seriesNum != null) {
            content.withSeriesNumber(seriesNum);
        }

        if (source.getSeriesOf() != null) {
            content.setParentRef(new ParentRef(BbcFeeds.nitroUriForPid(source.getSeriesOf()
                    .getPid())));
        }
        BigInteger expectedChildCount = source.getExpectedChildCount();
        if (expectedChildCount != null) {
            content.setTotalEpisodes(expectedChildCount.intValue());
        }
    }

    // Pattern based on data pulled from nitro records, not confirmed with clients. Series titles
    // with any characters after the series number are not matched to avoid creation of bad data.
    // i.e. match "Series 11", but do not match "Series 11: Reversions"
    private final Pattern seriesTitlePattern = Pattern.compile("^[Ss](eries|eason) ([0-9]{1,3})$");

    @Nullable
    private Integer getSeriesNumberFromTitle(String title) {

        if (title == null) {
            return null;
        }

        Matcher matcher = seriesTitlePattern.matcher(title);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(2));
            } catch (Exception e) {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    protected MasterBrand extractMasterBrand(Series source) {
        return source.getMasterBrand();
    }

}
