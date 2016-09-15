package org.atlasapi.remotesite.rt.extractors;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Author;
import org.atlasapi.media.entity.Review;

import com.google.common.base.Strings;
import nu.xom.Element;

public class RtReviewExtractor {

    private RtReviewExtractor() {

    }

    public static RtReviewExtractor create() {
        return new RtReviewExtractor();
    }

    public List<Review> extractReviews(Element filmElement) {
        List<Review> reviewList = new ArrayList<>();

        extractNormalReviews(filmElement).ifPresent(reviewList::add);
        extractFotdReviews(filmElement).ifPresent(reviewList::add);
        extractShortReview(filmElement).ifPresent(reviewList::add);
        extractWebsiteReview(filmElement).ifPresent(reviewList::add);

        return reviewList;
    }

    private Optional<Review> extractNormalReviews(Element filmElement) {
        Element reviewText = filmElement.getFirstChildElement("normal_review");
        Element authorName = filmElement.getFirstChildElement("review_author");
        Element authorInitials = filmElement.getFirstChildElement("review_author_initials");

        return extractReview(
                reviewText,
                authorName,
                authorInitials,
                "normal_review"
        );
    }

    private Optional<Review> extractFotdReviews(Element filmElement) {
        Element fotdReviewText = filmElement.getFirstChildElement("fotd_review");
        Element fotdAuthorName = filmElement.getFirstChildElement("fotd_review_author");
        Element fotdAuthorInitials = filmElement.getFirstChildElement("fotd_review_initials");

        return extractReview(
                fotdReviewText,
                fotdAuthorName,
                fotdAuthorInitials,
                "fotd_review"
        );
    }

    private Optional<Review> extractShortReview(Element filmElement) {
        Element shortReviewText = filmElement.getFirstChildElement("short_review");
        Element shortAuthorName = filmElement.getFirstChildElement("short_review_author");
        Element shortAuthorInitials = filmElement.getFirstChildElement("short_review_initials");

        return extractReview(
                shortReviewText,
                shortAuthorName,
                shortAuthorInitials,
                "short_review"
        );
    }

    private Optional<Review> extractWebsiteReview(Element filmElement) {
        Element websiteReviewText = filmElement.getFirstChildElement("website_review");
        Element websiteAuthorName = filmElement.getFirstChildElement("website_review_author");
        Element websiteAuthorInitials = filmElement.getFirstChildElement("website_review_initials");

        return extractReview(
                websiteReviewText,
                websiteAuthorName,
                websiteAuthorInitials,
                "website_review"
        );
    }

    private Optional<Review> extractReview(
            Element reviewText,
            Element authorName,
            Element authorInitials,
            String type
    ) {
        if (hasValue(reviewText) &&
                hasValue(authorName) &&
                hasValue(authorInitials)
                ) {
            return Optional.of(makeReview(
                    reviewText.getValue(),
                    authorName.getValue(),
                    authorInitials.getValue(),
                    type
            ));
        } else {
            return Optional.empty();
        }
    }

    private Review makeReview(
            String reviewText,
            String authorName,
            String authorInitials,
            String type
    ) {
        Review review = new Review(Locale.ENGLISH, reviewText);
        review.setAuthor(Author.builder()
                .withAuthorInitials(authorInitials)
                .withAuthorName(authorName)
                .build()
        );
        review.setType(type);

        return review;
    }

    public boolean hasValue(@Nullable Element subtitlesElement) {
        return subtitlesElement != null && !Strings.isNullOrEmpty(subtitlesElement.getValue());
    }
}
