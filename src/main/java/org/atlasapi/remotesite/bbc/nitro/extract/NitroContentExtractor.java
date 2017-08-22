package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.atlasapi.feeds.radioplayer.RadioPlayerServices;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.ion.BbcIonServices;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.time.Clock;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Template extractor for extracting {@link Content} from Nitro sources.
 * <p>
 * Concrete implementations must override methods to create the intended type of
 * {@code Content} and project required fields out of the source.
 *
 * @param <SOURCE>  - the Nitro source type.
 * @param <CONTENT> - the {@link Content} type to be extracted.
 */
public abstract class NitroContentExtractor<SOURCE, CONTENT extends Content>
        implements ContentExtractor<SOURCE, CONTENT> {

    private static final Logger log = LoggerFactory.getLogger(NitroContentExtractor.class);

    private static final String PID_NAMESPACE = "gb:bbc:pid";
    private static final String URI_NAMESPACE = "uri";

    private final Clock clock;
    private final NitroImageExtractor imageExtractor
            = new NitroImageExtractor(1024, 576);

    public NitroContentExtractor(Clock clock) {
        this.clock = clock;
    }

    @Override
    public final CONTENT extract(SOURCE source) {
        DateTime now = clock.now();
        CONTENT content = createContent(source);
        content.setLastUpdated(now);
        String pid = extractPid(source);
        content.setCanonicalUri(BbcFeeds.nitroUriForPid(pid));
        content.setPublisher(Publisher.BBC_NITRO);
        content.setTitle(extractTitle(source));
        content.setAliases(ImmutableSet.of(
                new Alias(PID_NAMESPACE, pid),
                new Alias(URI_NAMESPACE, content.getCanonicalUri())
        ));
        Synopses synposes = extractSynopses(source);
        if (synposes != null) {
            content.setDescription(longestSynopsis(synposes));
            content.setShortDescription(synposes.getShort());
            content.setMediumDescription(synposes.getMedium());
            content.setLongDescription(synposes.getLong());
        }
        com.metabroadcast.atlas.glycerin.model.Brand.Images.Image srcImage = extractImage(source);

        List<String> genericBbcUrlList = createGenericBbcUrlList();

        if (srcImage != null && !Strings.isNullOrEmpty(srcImage.getTemplateUrl())) {
            Image image = imageExtractor.extract(srcImage);

            if(genericBbcUrlList.contains(image.getCanonicalUri())) {
                image.setType(ImageType.GENERIC_IMAGE_CONTENT_ORIGINATOR);
            }
            content.setImage(image.getCanonicalUri());
            content.setImages(ImmutableSet.of(image));
        }

        MasterBrand masterBrand = extractMasterBrand(source);
        content.setMediaType(computeMediaType(masterBrand));
        if (masterBrand != null) {
            String masterBrandChannel = BbcIonServices.getMasterBrand(masterBrand.getMid());
            content.setPresentationChannel(masterBrandChannel);
            if (masterBrandChannel == null) {
                log.warn("No master brand mapping found for " + pid);
            }
        }
        //TODO: genres from v2 API
        extractAdditionalFields(source, content, now);
        return content;
    }

    /**
     * Projects the masterbrand of the source data.
     *
     * @param source - the source data
     * @return - the masterbrand of the source data, or {@code null} if there is none.
     */
    protected abstract @Nullable MasterBrand extractMasterBrand(SOURCE source);

    /**
     * Creates a the raw {@code Content} object to be extracted.
     *
     * @param source - the source data, this can be used to determine the right type of
     *               {@code Content} to create.
     * @return - returns a {@link Content} object.
     */
    protected abstract @Nonnull CONTENT createContent(SOURCE source);

    /**
     * Projects the PID of the source data.
     *
     * @param source - the source data
     * @return - the PID of the source data, must not be {@code null}.
     */
    protected abstract @Nonnull String extractPid(SOURCE source);

    /**
     * Projects the title of the source data.
     *
     * @param source - the source data
     * @return - the title of the source data, or {@code null} if there is none.
     */
    protected abstract @Nullable String extractTitle(SOURCE source);

    /**
     * Projects the {@link Synopses} of the source data.
     *
     * @param source - the source data
     * @return - the synopses of the source data, or {@code null} if there is
     * none.
     */
    protected abstract @Nullable Synopses extractSynopses(SOURCE source);

    /**
     * Projects the {@link com.metabroadcast.atlas.glycerin.model.Brand.Contributions}
     * of the source data.
     *
     * @param source - the source data
     * @return - the contributors of the source data, or {@code null} if there is none.
     */

    protected abstract @Nullable Brand.Contributions extractContributions(SOURCE source);

    /**
     * Projects the {@link com.metabroadcast.atlas.glycerin.model.Image Image}
     * of the source data.
     *
     * @param source - the source data
     * @return - the image of the source data, or {@code null} if there is none.
     */
    protected abstract @Nullable Brand.Images.Image extractImage(SOURCE source);

    protected abstract @Nullable AvailableVersions extractVersions(SOURCE source);

    /**
     * Concrete implementations can override this method to perform additional
     * configuration of the extracted content from the source.
     *
     * @param source  - the source data.
     * @param content - the extracted content.
     * @param now     - the current time.
     */
    protected void extractAdditionalFields(SOURCE source, CONTENT content, DateTime now) {

    }

    private String longestSynopsis(Synopses synopses) {
        return Strings.emptyToNull(
                Objects.firstNonNull(synopses.getLong(),
                        Objects.firstNonNull(synopses.getMedium(),
                                Objects.firstNonNull(synopses.getShort(), ""))));
    }

    private MediaType computeMediaType(MasterBrand masterBrand) {
        if (masterBrand == null) {
            return MediaType.VIDEO;
        }

        return RadioPlayerServices.masterBrandIdToService.containsKey(masterBrand.getMid()) ?
               MediaType.AUDIO :
               MediaType.VIDEO;
    }

    private List<String> createGenericBbcUrlList() {

        return ImmutableList.of(
                //BBC Logo
                "http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png",

                //BBC Radio Station Images
                "http://ichef.bbci.co.uk/images/ic/1024x576/p01lcnwl.jpg",
                "http://ichef.bbci.co.uk/images/ic/1024x576/p01ty5y1.jpg",
                "http://ichef.bbci.co.uk/images/ic/1024x576/p01t0zdl.jpg"

        );
    }
}
