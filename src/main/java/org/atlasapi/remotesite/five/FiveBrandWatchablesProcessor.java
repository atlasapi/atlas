package org.atlasapi.remotesite.five;

import java.io.InputStream;
import java.io.StringReader;
import java.util.Optional;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import nu.xom.Element;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.media.entity.Specialization.FILM;

public class FiveBrandWatchablesProcessor extends FiveBrandProcessor{

    private static final Logger log = LoggerFactory.getLogger(FiveBrandWatchablesProcessor.class);
    private final FiveWatchableProcessor watchableProcessor;

    protected FiveBrandWatchablesProcessor(Builder builder) {
        super(
                builder.writer,
                builder.contentResolver,
                builder.baseApiUrl,
                builder.httpClient,
                builder.channelMap,
                builder.locationPolicyIds
        );

        watchableProcessor = FiveWatchableProcessor.create(
                builder.baseApiUrl,
                builder.httpClient,
                builder.channelMap,
                builder.locationPolicyIds
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int processShow(Element transmission) {
        Element element = transmission
                .getFirstChildElement("watchable_link")
                .getFirstChildElement("watchable");

        String showLink = element.getFirstChildElement("show_link").getAttributeValue("href");
        String id = showLink.split("internal/shows/")[1];
        Brand brand = extractBrand(element, id);

        EpisodeProcessingNodeFactory nodeFactory = EpisodeProcessingNodeFactory.create(
                brand, watchableProcessor
        );

        try {
            HttpGet request = new HttpGet(getShowUri(id) + WATCHABLES_URL_SUFFIX);
            HttpResponse response = httpClient.execute(request);
            InputStream responseBody = response.getEntity().getContent();

            new nu.xom.Builder(nodeFactory).build(responseBody);
        } catch(Exception e) {
            log.error(
                    "Exception parsing episodes for brand " + brand.getTitle() + "with id " + id,
                    e
            );
            return 0;
        }

        if(FILM.equals(brand.getSpecialization()) && nodeFactory.items.size() == 1) {
            setFilmDescription((Film) Iterables.getOnlyElement(nodeFactory.items), element);
        }

        write(brand);

        watchableProcessor.getSeriesMap()
                .values()
                .forEach(this::write);

        nodeFactory.items
                .forEach(item -> write(brand, item));

        return nodeFactory.items.size();
    }

    private Brand extractBrand(Element element, String id) {
        String uri = getShowUri(id);

        Optional<Identified> optionalBrand = contentResolver.findByCanonicalUris(
                ImmutableSet.of(uri)
        )
                .getFirstValue().toOptional();

        Brand brand = createBrand(element, id);

        return optionalBrand.map(identified -> mergeBrand((Brand) identified, brand)).orElse(brand);
    }

    private Brand createBrand(Element element, String id) {
        String uri = getShowUri(id);

        Brand brand = new Brand(uri, getBrandCurie(id), Publisher.FIVE);
        brand.setTitle(childValue(element, "title"));
        brand.setDescription(getDescription(element).valueOrNull());
        brand.setGenres(getGenres(element));

        Optional<Image> imageMaybe = getImage(element).toOptional();

        if (imageMaybe.isPresent()) {
            Image image = imageMaybe.get();
            brand.setImage(image.getCanonicalUri());
            brand.setImages(ImmutableSet.of(image));
        } else {
            brand.setImage(null);
        }

        brand.setMediaType(MediaType.VIDEO);
        brand.setSpecialization(specializationFrom(element));

        return brand;
    }

    public static final class Builder {


        private ContentWriter writer;
        private ContentResolver contentResolver;
        private String baseApiUrl;
        private CloseableHttpClient httpClient;
        private Multimap<String, Channel> channelMap;
        private FiveLocationPolicyIds locationPolicyIds;

        private Builder() {
        }

        public Builder withWriter(ContentWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withContentResolver(ContentResolver contentResolver) {
            this.contentResolver = contentResolver;
            return this;
        }

        public Builder withBaseApiUrl(String baseApiUrl) {
            this.baseApiUrl = baseApiUrl;
            return this;
        }

        public Builder withHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder withChannelMap(Multimap<String, Channel> channelMap) {
            this.channelMap = channelMap;
            return this;
        }

        public Builder withLocationPolicyIds(FiveLocationPolicyIds locationPolicyIds) {
            this.locationPolicyIds = locationPolicyIds;
            return this;
        }

        public FiveBrandWatchablesProcessor build() {
            return new FiveBrandWatchablesProcessor(this);
        }
    }
}
