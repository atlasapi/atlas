package org.atlasapi.output.simple;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.feeds.utils.DescriptionWatermarker;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReviewType;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.simple.Award;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Image;
import org.atlasapi.media.entity.simple.LocalizedDescription;
import org.atlasapi.media.entity.simple.LocalizedTitle;
import org.atlasapi.media.entity.simple.Priority;
import org.atlasapi.media.entity.simple.PriorityScoreReasons;
import org.atlasapi.media.entity.simple.Rating;
import org.atlasapi.media.entity.simple.RelatedLink;
import org.atlasapi.media.entity.simple.Review;
import org.atlasapi.media.entity.simple.SameAs;
import org.atlasapi.output.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

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

    protected DescribedModelSimplifier(
            ImageSimplifier imageSimplifier,
            NumberToShortStringCodec codec,
            @Nullable DescriptionWatermarker descriptionWatermarker
    ) {
        super(codec);
        this.imageSimplifier = imageSimplifier;
        this.descriptionWatermarker = descriptionWatermarker;
    }

    protected void copyBasicDescribedAttributes(
            F content,
            T simpleDescription,
            Set<Annotation> annotations
    ) {

        copyIdentifiedAttributesTo(content, simpleDescription, annotations);

        if (annotations.contains(Annotation.DESCRIPTION) || annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            simpleDescription.setPublisher(toPublisherDetails(content.getPublisher()));

            simpleDescription.setTitle(content.getTitle());
            simpleDescription.setTitles(simplifyLocalizedTitles(content));
            simpleDescription.setDescription(applyWatermark(content, content.getDescription()));
            simpleDescription.setThumbnail(content.getThumbnail());
            simpleDescription.setImage(content.getImage());
            simpleDescription.setShortDescription(content.getShortDescription());
            simpleDescription.setAwards(simplifyAwards(content.getAwards()));

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
            if (content.getPriority() != null) {
                simpleDescription.setPriority(
                        new Priority(
                                content.getPriority().getScore(),
                                new PriorityScoreReasons(
                                    content.getPriority().getReasons().getPositive(),
                                    content.getPriority().getReasons().getNegative()
                                )
                        )
                );
            }
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

    private Function<LookupRef, SameAs> TO_SAME_AS = input -> {
        Long id = input.id();
        if (id == null) {
            log.info("null id for {}", input);
        }
        return new SameAs(id != null ? idCodec.encode(BigInteger.valueOf(id)) : null, input.uri());
    };

    private Iterable<RelatedLink> simplifyRelatedLinks(F described) {
        return Iterables.transform(described.getRelatedLinks(), rl -> {
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
        });
    }

    private Iterable<Review> simplifyReviews(final F content) {
        return content.getReviews().stream()
                .map(complex -> {
                    Review simple = new Review();

                    if (complex.getLocale() != null) {
                        simple.setLanguage(complex.getLocale().toLanguageTag());
                    }
                    simple.setReview(complex.getReview());
                    simple.setReviewType(ReviewType.fromKey(complex.getReviewTypeKey()));
                    simple.setDate(complex.getDate());
                    simple.setAuthor(complex.getAuthor());
                    simple.setAuthorInitials(complex.getAuthorInitials());
                    simple.setRating(complex.getRating());
                    simple.setPublisherDetails(
                            toPublisherDetails(
                                    Publisher.fromKey(complex.getPublisherKey())
                                            .valueOrDefault(content.getPublisher())
                            )
                    );

                    return simple;
                })
                .collect(MoreCollectors.toImmutableList());
    }


    private Set<LocalizedDescription> simplifyLocalizedDescriptions(F content) {
        return ImmutableSet.copyOf(Iterables.transform(content.getLocalizedDescriptions(),
                TO_SIMPLE_LOCALISED_DESCRIPTION));
    }

    private Set<LocalizedTitle> simplifyLocalizedTitles(F content) {
        return ImmutableSet.copyOf(Iterables.transform(content.getLocalizedTitles(),
                TO_SIMPLE_LOCALIZED_TITLE));
    }

    private Set<Award> simplifyAwards(Set<org.atlasapi.media.entity.Award> awards) {
        return awards.stream()
                .map(input -> {
                    Award award = new Award();
                    award.setDescription(input.getDescription());
                    award.setOutcome(input.getOutcome());
                    award.setYear(input.getYear());
                    award.setTitle(input.getTitle());
                    return award;
                })
                .collect(Collectors.toSet());
    }

    private static final Function<org.atlasapi.media.entity.LocalizedDescription, LocalizedDescription> TO_SIMPLE_LOCALISED_DESCRIPTION = complex -> {
        LocalizedDescription simple = new LocalizedDescription();

        //ignore NPE warning because it's triggered by complex possibly being null, which it likely won't
        if(complex.getLocale() != null) {
            simple.setLanguage(complex.getLocale().getLanguage());
            simple.setRegion(complex.getLocale().getCountry());
        }
        simple.setDescription(complex.getDescription());
        simple.setLongDescription(complex.getLongDescription());
        simple.setMediumDescription(complex.getMediumDescription());
        simple.setShortDescription(complex.getShortDescription());

        return simple;
    };

    private static final Function<org.atlasapi.media.entity.LocalizedTitle, LocalizedTitle> TO_SIMPLE_LOCALIZED_TITLE = complex -> {
        LocalizedTitle simple = new LocalizedTitle();

        //ignore NPE because it's triggered by complex possibly being null, which it likely won't
        if(complex.getLocale() != null) {
            simple.setLanguage(complex.getLocale().getLanguage());
            simple.setRegion(complex.getLocale().getCountry());
        }
        simple.setTitle(complex.getTitle());

        return simple;
    };

}
