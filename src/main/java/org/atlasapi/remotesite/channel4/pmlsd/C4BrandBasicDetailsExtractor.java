package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.ContentExtractor;
import org.jdom.Element;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Category;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts basic properties of a Brand from a C4 Atom Feed representing a Brand.
 *
 * @author Fred van den Driessche (fred@metabroadcast.com)
 */
public class C4BrandBasicDetailsExtractor implements ContentExtractor<Feed, Brand> {

    Logger log = LoggerFactory.getLogger(C4BrandBasicDetailsExtractor.class);

    private static final String PRESENTATION_BRAND = "relation.presentationBrand";

    private final C4AtomApi c4AtomApi;
    private final Clock clock;
    private final ContentFactory<Feed, Feed, Entry> contentFactory;

    public C4BrandBasicDetailsExtractor(C4AtomApi c4AtomApi, 
            ContentFactory<Feed, Feed, Entry> contentFactory, Clock clock) {
        this.c4AtomApi = c4AtomApi;
        this.contentFactory = contentFactory;
        this.clock = clock;
    }

    @Override
    public Brand extract(Feed source) {
        Preconditions.checkArgument(C4AtomApi.isABrandFeed(source), "Not a brand feed");

        Brand brand = contentFactory.createBrand(source).get();
        brand.setLastUpdated(clock.now());

        // TODO new alias
        brand.addAliasUrl(canonicalIdTag(source));
        brand.addAliasUrl(C4AtomApi.hierarchyUriFromCanonical(brand.getCanonicalUri()));

        brand.setTitle(source.getTitle());
        brand.setDescription(source.getSubtitle().getValue());

        brand.setMediaType(MediaType.VIDEO);
        brand.setSpecialization(Specialization.TV);

        Set<String> genres = Sets.newHashSet();
        if (source.getCategories() != null) {
            for (Object cat : source.getCategories()) {
                String category = canonicalise((Category) cat);
                if (category != null) {
                    genres.add(category);
                }
            }
        }
        brand.setGenres(new C4CategoryGenreMap().mapRecognised(genres));

        String presentationBrand = getPresentationBrand(source);
        if(presentationBrand != null) {
            Channel channel = c4AtomApi.getChannelMap().get(presentationBrand);
            if (channel != null) {
                brand.setPresentationChannel(channel);
            } else {
                log.warn(String.format(
                        "Tried to set presentation channel for brand (%s), but could not find %s in"
                        + " the channel map.",
                        brand.getTitle(),
                        presentationBrand
                ));
            }
        }

        C4AtomApi.addImages(brand, source.getLogo());

        return brand;
    }

    private String canonicalIdTag(Feed source) {
        return C4AtomApi.canonicalizeBrandFeedId(source);
    }

    private final Pattern CATEGORY_PATTERN = Pattern.compile("https?://[^.]+\\.channel4\\.com/[^/]+/(categories/[^.]+)\\.atom.*");

    private String canonicalise(Category category) {
        Matcher matcher = CATEGORY_PATTERN.matcher(category.getTerm());
        if (matcher.matches()) {
            return C4AtomApi.WEB_BASE + matcher.group(1);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private String getPresentationBrand(Feed source) {
        Iterable<Element> markup = (Iterable<Element>) source.getForeignMarkup();
        for (Element element : markup) {
            if (PRESENTATION_BRAND.equals(element.getName())) {
                return element.getValue();
            }
        }
        return null;
    }

}
