package org.atlasapi.remotesite.bbc.nitro.extract;

import java.math.BigInteger;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.joda.time.DateTime;

import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.Image;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Series;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.time.Clock;

/**
 * A {@link NitroContentExtractor} for extracting
 * {org.atlasapi.media.entity.Series Atlas Series} from {@link Series Nitro
 * Series}.
 * 
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

    @Override protected Brand.People extractPeople(Series series) {
        return series.getPeople();
    }

    @Override
    protected Image extractImage(Series source) {
        return source.getImage();
    }

    @Override
    protected void extractAdditionalFields(Series source, org.atlasapi.media.entity.Series content, DateTime now) {
        if (source.getSeriesOf() != null) {
            BigInteger position = source.getSeriesOf().getPosition();
            if (position != null) {
                content.withSeriesNumber(position.intValue());
            }
            content.setParentRef(new ParentRef(BbcFeeds.nitroUriForPid(source.getSeriesOf().getPid())));
        }
        BigInteger expectedChildCount = source.getExpectedChildCount();
        if (expectedChildCount != null) {
            content.setTotalEpisodes(expectedChildCount.intValue());
        }
    }

    @Override
    protected MasterBrand extractMasterBrand(Series source) {
        return source.getMasterBrand();
    }

}
