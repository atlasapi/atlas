package org.atlasapi.remotesite.bbc.nitro.extract;

import javax.annotation.Nullable;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.time.Clock;

/**
 * Extracts a {@link org.atlasapi.media.entity.Brand Atlas Brand} from a
 * {@link Brand Nitro Brand}.
 *
 * @see NitroContentExtractor
 */
public class NitroBrandExtractor
        extends NitroContentExtractor<Brand, org.atlasapi.media.entity.Brand> {

    public NitroBrandExtractor(Clock clock) {
        super(clock);
    }

    @Override
    protected org.atlasapi.media.entity.Brand createContent(Brand source) {
        return new org.atlasapi.media.entity.Brand();
    }

    @Override
    protected String extractPid(Brand source) {
        return source.getPid();
    }

    @Override
    protected String extractTitle(Brand source) {
        return source.getTitle();
    }

    @Override
    protected Synopses extractSynopses(Brand source) {
        return source.getSynopses();
    }

    @Override
    protected Brand.Contributions extractContributions(Brand brand) {
        return brand.getContributions();
    }

    @Override
    protected Brand.Images.Image extractImage(Brand source) {
        if (source.getImages() == null) {
            return null;
        }
        return source.getImages().getImage();
    }

    @Nullable
    @Override
    protected AvailableVersions extractVersions(Brand brand) {
        return null;
    }

    @Override
    protected MasterBrand extractMasterBrand(Brand source) {
        return source.getMasterBrand();
    }

}
