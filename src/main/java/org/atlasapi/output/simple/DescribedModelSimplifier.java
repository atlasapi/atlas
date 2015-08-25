package org.atlasapi.output.simple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.feeds.utils.DescriptionWatermarker;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.simple.*;
import org.atlasapi.output.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public abstract class DescribedModelSimplifier<F extends Described, T extends Description> extends IdentifiedModelSimplifier<F,T> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ImageSimplifier imageSimplifier;
    private final DescriptionWatermarker descriptionWatermarker;
    private final DescribedImageExtractor imageExtractor = new DescribedImageExtractor();
    private final PublisherSimplifier publisherSimplifier = new PublisherSimplifier();
    private final RatingModelSimplifier ratingModelSimplifier = new RatingModelSimplifier();
    private final AudienceStatisticsModelSimplifier audienceStatsModelSimplifier = new AudienceStatisticsModelSimplifier();
    
    protected DescribedModelSimplifier(ImageSimplifier imageSimplifier) {
        this.imageSimplifier = imageSimplifier;
        this.descriptionWatermarker = null;
    }
    
    protected DescribedModelSimplifier(ImageSimplifier imageSimplifier, NumberToShortStringCodec codec, 
            @Nullable DescriptionWatermarker descriptionWatermarker) {
        super(codec);
        this.imageSimplifier = imageSimplifier;
        this.descriptionWatermarker = descriptionWatermarker;
    }
    
    protected void copyBasicDescribedAttributes(F content, T simpleDescription, Set<Annotation> annotations) {
        
        copyIdentifiedAttributesTo(content, simpleDescription, annotations);
        
        if (annotations.contains(Annotation.DESCRIPTION) || annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            simpleDescription.setPublisher(toPublisherDetails(content.getPublisher()));
            
            simpleDescription.setTitle(content.getTitle());
            simpleDescription.setTitles(simplifyLocalizedTitles(content));
            simpleDescription.setDescription(applyWatermark(content, content.getDescription()));
            simpleDescription.setImage(content.getImage());
            simpleDescription.setThumbnail(content.getThumbnail());
            simpleDescription.setShortDescription(content.getShortDescription());

            MediaType mediaType = content.getMediaType();
            if (mediaType != null) {
                simpleDescription.setMediaType(mediaType.toString().toLowerCase());
            }
            
            Specialization specialization = content.getSpecialization();
            if (specialization != null) {
                simpleDescription.setSpecialization(specialization.toString().toLowerCase());
            }
        }
        
        if (annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            simpleDescription.setGenres(content.getGenres());
            simpleDescription.setTags(content.getTags());
            simpleDescription.setSameAs(Iterables.transform(content.getEquivalentTo(), LookupRef.TO_URI));
            simpleDescription.setEquivalents(Iterables.transform(content.getEquivalentTo(), TO_SAME_AS));
            simpleDescription.setPresentationChannel(content.getPresentationChannel());
            simpleDescription.setMediumDescription(applyWatermark(content, content.getMediumDescription()));
            simpleDescription.setLongDescription(applyWatermark(content, content.getLongDescription()));
            simpleDescription.setDescriptions(simplifyLocalizedDescriptions(content));
            simpleDescription.setPriority(new Priority(content.getPriority().getScore(),
                content.getPriority().getReasons()));
        }
        
        if (annotations.contains(Annotation.IMAGES)) {
            simpleDescription.setImages(toImages(imageExtractor.getImages(content), annotations));
        }
        if (annotations.contains(Annotation.RELATED_LINKS)) {
            simpleDescription.setRelatedLinks(simplifyRelatedLinks(content));
        }
        if (annotations.contains(Annotation.REVIEWS)) {
            simpleDescription.setReviews(simplifyReviews(content));
        }
        if (annotations.contains(Annotation.AUDIENCE_STATISTICS)) {
            simpleDescription.setAudienceStatistics(
                    audienceStatsModelSimplifier.simplify(content.getAudienceStatistics(), annotations, null));
        }
        if (annotations.contains(Annotation.RATINGS)) {
            simpleDescription.setRatings(toRatings(content.getRatings(), annotations));
        }
        
    }

    private String applyWatermark(F described, String description) {
        if (!(described instanceof Item) || descriptionWatermarker == null) {
            return description;
        }
        
        Item item = (Item) described;
        Broadcast firstBroadcast = Iterables.getFirst(Item.FLATTEN_BROADCASTS.apply(item), null);
        
        return descriptionWatermarker.watermark(firstBroadcast, description);
    }
    
    private Iterable<Image> toImages(Iterable<org.atlasapi.media.entity.Image> images, Set<Annotation> annotations) {
        Builder<Image> simpleImages = ImmutableSet.builder();
        for(org.atlasapi.media.entity.Image image : images) {
            simpleImages.add(imageSimplifier.simplify(image, annotations, null));
        }
        return simpleImages.build();
    }
    
    private Iterable<Rating> toRatings(Iterable<org.atlasapi.media.entity.Rating> ratings, Set<Annotation> annotations) {
        ImmutableList.Builder<Rating> simpleRatings = ImmutableList.builder();
        for (org.atlasapi.media.entity.Rating rating : ratings) {
            simpleRatings.add(ratingModelSimplifier.simplify(rating, annotations, null));
        }
        return simpleRatings.build();
    }
    
    private Function<LookupRef, SameAs> TO_SAME_AS = new Function<LookupRef, SameAs>() {

        @Override
        public SameAs apply(LookupRef input) {
            Long id = input.id();
            if (id == null) {
                log.info("null id for {}", input);
            }
            return new SameAs(id != null ? idCodec.encode(BigInteger.valueOf(id)) : null, input.uri());
        }
    };
    
    private Iterable<RelatedLink> simplifyRelatedLinks(F described) {
        return Iterables.transform(described.getRelatedLinks(), new Function<org.atlasapi.media.entity.RelatedLink, RelatedLink>() {

            @Override
            public RelatedLink apply(org.atlasapi.media.entity.RelatedLink rl) {
                RelatedLink simpleLink = new RelatedLink();

                simpleLink.setUrl(rl.getUrl());
                simpleLink.setType(rl.getType().toString().toLowerCase());
                simpleLink.setSourceId(rl.getSourceId());
                simpleLink.setShortName(rl.getShortName());
                simpleLink.setTitle(rl.getTitle());
                simpleLink.setDescription(rl.getDescription());
                simpleLink.setImage(rl.getImage());
                simpleLink.setThumbnail(rl.getThumbnail());

                return simpleLink;
            }
        });
    }
    
    private Iterable<Review> simplifyReviews(final F content) {
        return Iterables.transform(content.getReviews(), new Function<org.atlasapi.media.entity.Review, Review>() {

            @Override
            public Review apply(org.atlasapi.media.entity.Review complex) {
                Review simple = new Review();
                
                if (complex.getLocale() != null) {
                    simple.setLanguage(complex.getLocale().toLanguageTag());
                }
                simple.setReview(complex.getReview());
                simple.setPublisherDetails(toPublisherDetails(content.getPublisher()));
                
                return simple;
            }
        });
    }

    
    private Set<LocalizedDescription> simplifyLocalizedDescriptions(F content) {
        return ImmutableSet.copyOf(Iterables.transform(content.getLocalizedDescriptions(),
                TO_SIMPLE_LOCALISED_DESCRIPTION));
    }

    private Set<LocalizedTitle> simplifyLocalizedTitles(F content) {
        return ImmutableSet.copyOf(Iterables.transform(content.getLocalizedTitles(),
                TO_SIMPLE_LOCALIZED_TITLE));
    }
    
    private static final Function<org.atlasapi.media.entity.LocalizedDescription, LocalizedDescription> TO_SIMPLE_LOCALISED_DESCRIPTION = new Function<org.atlasapi.media.entity.LocalizedDescription, LocalizedDescription>() {

        @Override
        public LocalizedDescription apply(org.atlasapi.media.entity.LocalizedDescription complex) {
            LocalizedDescription simple = new LocalizedDescription();

            simple.setLanguage(complex.getLanguageTag());
            simple.setDescription(complex.getDescription());
            simple.setLongDescription(complex.getLongDescription());
            simple.setMediumDescription(complex.getMediumDescription());
            simple.setShortDescription(complex.getShortDescription());

            return simple;
        }
    };

    private static final Function<org.atlasapi.media.entity.LocalizedTitle, LocalizedTitle> TO_SIMPLE_LOCALIZED_TITLE = new Function<org.atlasapi.media.entity.LocalizedTitle, LocalizedTitle>() {

        @Override
        public LocalizedTitle apply(org.atlasapi.media.entity.LocalizedTitle complex) {
            LocalizedTitle simple = new LocalizedTitle();

            simple.setLanguage(complex.getLanguageTag());
            simple.setTitle(complex.getTitle());

            return simple;
        }
    };
    
}
