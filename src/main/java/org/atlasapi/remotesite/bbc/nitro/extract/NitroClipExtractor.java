package org.atlasapi.remotesite.bbc.nitro.extract;

import javax.annotation.Nullable;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Clip;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.time.Clock;

/**
 * Extracts a {@link org.atlasapi.media.entity.Clip Atlas Clip} from a
 * {@link Clip}.
 * <p>
 * The "{@link org.atlasapi.media.entity.Clip#getClipOf clip of}" field is not
 * set.
 */
public class NitroClipExtractor
        extends BaseNitroItemExtractor<Clip, org.atlasapi.media.entity.Clip> {

    public NitroClipExtractor(Clock clock) {
        super(clock);
    }

    @Override
    protected org.atlasapi.media.entity.Clip createContent(NitroItemSource<Clip> source) {
        return new org.atlasapi.media.entity.Clip();
    }

    @Override
    protected String extractPid(NitroItemSource<Clip> source) {
        return source.getProgramme().getPid();
    }

    @Override
    protected String extractTitle(NitroItemSource<Clip> source) {
        return source.getProgramme().getTitle();
    }

    @Override
    protected Synopses extractSynopses(NitroItemSource<Clip> source) {
        return source.getProgramme().getSynopses();
    }

    @Override
    protected Brand.Contributions extractContributions(NitroItemSource<Clip> source) {
        return source.getProgramme().getContributions();
    }

    @Override
    protected Brand.Images.Image extractImage(NitroItemSource<Clip> source) {
        return source.getProgramme().getImages().getImage();
    }

    @Nullable
    @Override
    protected AvailableVersions extractVersions(NitroItemSource<Clip> clipNitroItemSource) {
        return clipNitroItemSource.getProgramme().getAvailableVersions();
    }

    @Override
    protected String extractMediaType(NitroItemSource<Clip> source) {
        return source.getProgramme().getMediaType();
    }

    @Override
    protected MasterBrand extractMasterBrand(NitroItemSource<Clip> source) {
        return source.getProgramme().getMasterBrand();
    }

}
