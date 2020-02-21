package org.atlasapi.input;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.Award;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Priority;
import org.atlasapi.media.entity.PriorityScoreReasons;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.Builder;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Image;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Locale;
import java.util.Set;

public abstract class DescribedModelTransformer<F extends Description,T extends Described> extends IdentifiedModelTransformer<F, T> {

    public DescribedModelTransformer(Clock clock) {
        super(clock);
    }

    @Override
    protected final T createIdentifiedOutput(F simple, DateTime now) {
        T result = createOutput(simple);
        return setFields(result, simple);

    }

    protected abstract T createOutput(F simple);


    protected T setFields(T result, F inputContent) {
        Publisher publisher = getPublisher(inputContent.getPublisher());
        result.setPublisher(publisher);
        result.setTitle(inputContent.getTitle());
        result.setLocalizedTitles(transformTitles(inputContent.getTitles()));
        result.setDescription(inputContent.getDescription());
        result.setShortDescription(inputContent.getShortDescription());
        result.setMediumDescription(inputContent.getMediumDescription());
        result.setLongDescription(inputContent.getLongDescription());
        result.setImage(inputContent.getImage());
        result.setThumbnail(inputContent.getThumbnail());
        result.setRelatedLinks(relatedLinks(inputContent.getRelatedLinks()));
        result.setImages(transformImages(inputContent.getImages()));
        if (inputContent.getPriority() != null) {
            result.setPriority(new Priority(inputContent.getPriority().getScore(),
                    new PriorityScoreReasons(
                            inputContent.getPriority().getReasons().getPositive(),
                            inputContent.getPriority().getReasons().getNegative()
                    )
            ));
        }
        if (inputContent.getSpecialization() != null) {
            result.setSpecialization(Specialization.fromKey(inputContent.getSpecialization()).valueOrNull());
        }
        if (inputContent.getMediaType() != null) {
            result.setMediaType(MediaType.valueOf(inputContent.getMediaType().toUpperCase()));
        }
        if (inputContent.getReviews() != null) {
            result.setReviews(reviews(result.getPublisher(), inputContent.getReviews()));
        }
        result.setAwards(transformAwards(inputContent.getAwards()));
        result.setPresentationChannel(inputContent.getPresentationChannel());
        result.setGenres(inputContent.getGenres());
        result.setRatings(transformRatings(inputContent.getRatings()));

        return result;
    }

    private Set<LocalizedTitle> transformTitles(
            Set<org.atlasapi.media.entity.simple.LocalizedTitle> simpleLocalizedTitles
    ) {
        return simpleLocalizedTitles.stream()
                .map(input -> {
                    LocalizedTitle localizedTitle = new LocalizedTitle();
                    localizedTitle.setTitle(input.getTitle());
                    return localizedTitle;
                })
                .collect(MoreCollectors.toImmutableSet());
    }

    private Iterable<org.atlasapi.media.entity.Image> transformImages(Set<Image> images) {
        if (images == null || images.isEmpty()) {
            return ImmutableList.of();
        }
        return Collections2.transform(
                images,
                input -> {
                    org.atlasapi.media.entity.Image transformedImage = new org.atlasapi.media.entity.Image(
                            input.getUri()
                    );

                    transformedImage.setHeight(input.getHeight());
                    transformedImage.setWidth(input.getWidth());

                    if (input.getType() != null) {
                        transformedImage.setType(ImageType.valueOf(input.getImageType().toUpperCase()));
                    }
                    return transformedImage;
                }
        );
    }

    private Iterable<RelatedLink> relatedLinks(
            List<org.atlasapi.media.entity.simple.RelatedLink> relatedLinks) {
        return Lists.transform(
                relatedLinks,
                input -> {
                    LinkType type = LinkType.valueOf(input.getType().toUpperCase());
                    Builder link = RelatedLink.relatedLink(type,input.getUrl())
                       .withSourceId(input.getSourceId())
                       .withShortName(input.getShortName())
                       .withTitle(input.getTitle())
                       .withDescription(input.getDescription())
                       .withImage(input.getImage())
                       .withThumbnail(input.getThumbnail());
                    return link.build();
                }
        );
    }

    private Iterable<Review> reviews(
            final Publisher contentPublisher,
            Set<org.atlasapi.media.entity.simple.Review> simpleReviews
    ) {
        return simpleReviews.stream()
                .map(simpleReview -> {
                    if (simpleReview.getPublisherDetails() != null &&
                            !getPublisher(simpleReview.getPublisherDetails()).equals(contentPublisher)) {
                        throw new IllegalArgumentException("Review publisher must match content publisher");
                    }

                    return Review.builder()
                            .withLocale(Locale.forLanguageTag(simpleReview.getLanguage()))
                            .withReview(simpleReview.getReview())
                            .withPublisherKey(simpleReview.getPublisherDetails().getKey())
                            .withDate(simpleReview.getDate())
                            .withAuthor(simpleReview.getAuthor())
                            .withAuthorInitials(simpleReview.getAuthorInitials())
                            .withRating(simpleReview.getRating())
                            .withReviewTypeKey(simpleReview.getReviewType().toKey())
                            .build();
                })
                .collect(MoreCollectors.toImmutableList());
    }

    protected Publisher getPublisher(PublisherDetails pubDets) {
        if (pubDets == null || pubDets.getKey() == null) {
            throw new IllegalArgumentException("missing publisher");
        }
        Maybe<Publisher> possiblePublisher = Publisher.fromKey(pubDets.getKey());
        if (possiblePublisher.isNothing()) {
            throw new IllegalArgumentException("unknown publisher " + pubDets.getKey());
        }
        return possiblePublisher.requireValue();
    }

    private Set<Award> transformAwards(Set<org.atlasapi.media.entity.simple.Award> awards) {
        return awards.stream()
                .map(input -> {
                    Award award = new Award();
                    award.setDescription(input.getDescription());
                    award.setOutcome(input.getOutcome());
                    award.setTitle(input.getTitle());
                    award.setYear(input.getYear());
                    return award;
                })
                .collect(MoreCollectors.toImmutableSet());
    }

    private Set<Rating> transformRatings(Set<org.atlasapi.media.entity.simple.Rating> ratings) {
        return ratings.stream()
                .map(input -> new Rating(
                                input.getType(),
                                input.getValue(),
                                getPublisher(input.getPublisherDetails())
                        )
                )
                .collect(MoreCollectors.toImmutableSet());
    }
}
