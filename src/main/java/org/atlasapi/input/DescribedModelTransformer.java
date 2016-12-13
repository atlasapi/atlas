package org.atlasapi.input;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import org.atlasapi.media.entity.Author;
import org.atlasapi.media.entity.Award;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Distribution;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Language;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Priority;
import org.atlasapi.media.entity.PriorityScoreReasons;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.Builder;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Image;
import org.atlasapi.media.entity.simple.Localized;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;

public abstract class DescribedModelTransformer<F extends Description, T extends Described>
        extends IdentifiedModelTransformer<F, T> {

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
            result.setSpecialization(
                    Specialization.fromKey(inputContent.getSpecialization()).valueOrNull()
            );
        }
        if (inputContent.getMediaType() != null) {
            result.setMediaType(MediaType.valueOf(inputContent.getMediaType().toUpperCase()));
        }
        if (inputContent.getReviews() != null) {
            result.setReviews(reviews(result.getPublisher(), inputContent.getReviews()));
        }
        result.setAwards(transformAwards(inputContent.getAwards()));
        result.setPresentationChannel(inputContent.getPresentationChannel());

        if (inputContent.getTitles() != null) {
            result.setLocalizedTitles(transformLocalizedTitles(inputContent.getTitles()));
        }
        if (inputContent.getDistributions() != null) {
            result.setDistributions(transformDistributions(inputContent.getDistributions()));
        }
        if (inputContent.getLanguage() != null) {
            result.setLanguage(transformLanguages(inputContent.getLanguage()));
        }

        return result;
    }

    private Language transformLanguages(org.atlasapi.media.entity.simple.Language language) {
        return Language.builder()
                .withCode(language.getCode())
                .withDisplay(language.getDisplay())
                .withDubbing(language.getDubbing())
                .build();
    }

    private Distribution makeNewDistribution(
            org.atlasapi.media.entity.simple.Distribution distribution
    ) {
        return Distribution.builder()
                .withDistributor(distribution.getDistributor())
                .withFormat(distribution.getFormat())
                .withReleaseDate(distribution.getReleaseDate())
                .build();
    }

    private Iterable<Distribution> transformDistributions(
            Iterable<org.atlasapi.media.entity.simple.Distribution> distributions
    ) {
        List<Distribution> newDistributions = new ArrayList<Distribution>();
        for (org.atlasapi.media.entity.simple.Distribution distribution :
                distributions) {

            Distribution newDistribution = makeNewDistribution(distribution);
            newDistributions.add(newDistribution);
        }

        return ImmutableList.<Distribution>copyOf(newDistributions);
    }

    private LocalizedTitle makeNewTitle(org.atlasapi.media.entity.simple.LocalizedTitle title) {
        LocalizedTitle newTitle = new LocalizedTitle();
        newTitle.setTitle(title.getTitle());
        newTitle.setLocale(new Locale(title.getLanguage()));
        newTitle.setType(title.getType());
        return newTitle;
    }

    private Set<LocalizedTitle> transformLocalizedTitles(
            Set<org.atlasapi.media.entity.simple.LocalizedTitle> titles
    ) {
        return titles.stream()
                .map(title -> makeNewTitle(title))
                .collect(MoreCollectors.toImmutableSet());
    }

    private Iterable<org.atlasapi.media.entity.Image> transformImages(Set<Image> images) {
        if (images == null) {
            return ImmutableList.of();
        }
        return Collections2.transform(
                images,
                new Function<Image, org.atlasapi.media.entity.Image>() {
            @Override
            public org.atlasapi.media.entity.Image apply(Image input) {
                org.atlasapi.media.entity.Image transformedImage =
                        new org.atlasapi.media.entity.Image(
                                input.getUri()
                        );
                transformedImage.setHeight(input.getHeight());
                transformedImage.setWidth(input.getWidth());
                if (input.getType() != null) {
                    transformedImage.setType(ImageType.valueOf(input.getImageType().toUpperCase()));
                }
                return transformedImage;
            }
        });
    }

    private Iterable<RelatedLink> relatedLinks(
            List<org.atlasapi.media.entity.simple.RelatedLink> relatedLinks) {
        return Lists.transform(relatedLinks,
            new Function<org.atlasapi.media.entity.simple.RelatedLink, RelatedLink>() {
                @Override
                public RelatedLink apply(org.atlasapi.media.entity.simple.RelatedLink input) {
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
            }
        );
    }

    private Iterable<Review> reviews(final Publisher contentPublisher,
            Set<org.atlasapi.media.entity.simple.Review> simpleReviews) {
        return Iterables.transform(simpleReviews,
                simpleReview -> {
                    if (simpleReview.getPublisherDetails() != null &&
                            !getPublisher(simpleReview.getPublisherDetails())
                                    .equals(contentPublisher)) {
                        throw new IllegalArgumentException(
                                "Review publisher must match content publisher"
                        );
                    }
                    Review review = new Review(Locale.forLanguageTag(
                            simpleReview.getLanguage()),
                            simpleReview.getReview()
                    );
                    review.setType(simpleReview.getType());

                    Author author = Author.builder()
                            .withAuthorName(simpleReview.getAuthor().getAuthorName())
                            .withAuthorInitials(simpleReview.getAuthor().getAuthorInitials())
                            .build();
                    review.setAuthor(author);

                    return review;
                }
        );
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
        return FluentIterable.from(awards)
                .transform(new Function<org.atlasapi.media.entity.simple.Award, Award>() {

                    @Override
                    public Award apply(org.atlasapi.media.entity.simple.Award input) {
                        Award award = new Award();
                        award.setDescription(input.getDescription());
                        award.setOutcome(input.getOutcome());
                        award.setTitle(input.getTitle());
                        award.setYear(input.getYear());
                        return award;
                    }
                })
                .toSet();
    }
}
