package org.atlasapi.query.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.*;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;


public class ContentWriteMergerTest {

    private ContentWriteMerger contentWriteMerger = ContentWriteMerger.create();

    @Test
    public void localizedTitlesMergingTest() {

        Item contentOne = makeItem();
        Item contentTwo = makeItem();

        assertLocalizedTitleMergeCount(
                contentOne.copy(),
                contentTwo.copy(),
                0,
                true
        );
        assertLocalizedTitleMergeCount(
                contentTwo.copy(),
                contentOne.copy(),
                0,
                false
        );

        contentOne.setLocalizedTitles(makeLocalizedTitles(
                "some type",
                "england",
                "some title"
        ));

        // copys are used as the merge method alters and returns the first parameter
        assertLocalizedTitleMergeCount(contentOne.copy(), contentTwo.copy(), 1, true);
        assertLocalizedTitleMergeCount(contentTwo.copy(), contentOne.copy(), 1, true);
        assertLocalizedTitleMergeCount(contentOne.copy(), contentTwo.copy(), 0, false);
        assertLocalizedTitleMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

        contentTwo.setLocalizedTitles(makeLocalizedTitles(
                "some type two",
                "usa",
                "some title two"
        ));

        assertLocalizedTitleMergeCount(contentOne.copy(), contentTwo.copy(), 2, true);
        assertLocalizedTitleMergeCount(contentOne.copy(), contentTwo.copy(), 1, false);
        assertLocalizedTitleMergeCount(contentTwo.copy(), contentOne.copy(), 2, true);
        assertLocalizedTitleMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

    }



    @Test
    public void reviewsMergingTest() {
        Item contentOne = makeItem();
        Item contentTwo = makeItem();

        assertReviewMergeCount(contentOne.copy(), contentTwo.copy(), 0, true);
        assertReviewMergeCount(contentTwo.copy(), contentOne.copy(), 0, false);

        contentOne.setReviews(makeReviews(
                "england",
                "review",
                "some initials",
                "some name")
        );

        assertReviewMergeCount(contentOne.copy(), contentTwo.copy(), 1, true);
        assertReviewMergeCount(contentTwo.copy(), contentOne.copy(), 1, true);
        assertReviewMergeCount(contentOne.copy(), contentTwo.copy(), 0, false);
        assertReviewMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

        contentTwo.setReviews(makeReviews(
                "usa",
                "some review",
                "other initials",
                "other name"
        ));

        assertReviewMergeCount(contentOne.copy(), contentTwo.copy(), 2, true);
        assertReviewMergeCount(contentTwo.copy(), contentOne.copy(), 2, true);
        assertReviewMergeCount(contentOne.copy(), contentTwo.copy(), 1, false);
        assertReviewMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

        Optional<Identified> optionalContentOne = Optional.of(contentOne.copy());
        Content mergeResult = contentWriteMerger.merge(optionalContentOne, contentTwo.copy(), true);

        assertEquals(ImmutableSet.copyOf(Iterables.concat(
                contentTwo.getReviews(),
                contentOne.getReviews()
                )), mergeResult.getReviews()
        );
    }

    @Test
    public void distributionsMergingTest() {
        Item contentOne = makeItem();
        Item contentTwo = makeItem();

        assertDistributionsMergeCount(contentOne.copy(), contentTwo.copy(), 0, true);
        assertDistributionsMergeCount(contentTwo.copy(), contentOne.copy(), 0, false);

        contentOne.setDistributions(ImmutableList.of(Distribution.builder()
                .withDistributor("distributor")
                .withFormat("format")
                .withReleaseDate(DateTime.now())
                .build()
        ));

        assertDistributionsMergeCount(contentOne.copy(), contentTwo.copy(), 1, true);
        assertDistributionsMergeCount(contentTwo.copy(), contentOne.copy(), 1, true);
        assertDistributionsMergeCount(contentOne.copy(), contentTwo.copy(), 0, false);
        assertDistributionsMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

        contentTwo.setDistributions(ImmutableList.of(Distribution.builder()
                .withDistributor("other distributor")
                .withFormat("other format")
                .withReleaseDate(DateTime.now())
                .build()
        ));

        assertDistributionsMergeCount(contentOne.copy(), contentTwo.copy(), 2, true);
        assertDistributionsMergeCount(contentTwo.copy(), contentOne.copy(), 2, true);
        assertDistributionsMergeCount(contentOne.copy(), contentTwo.copy(), 1, false);
        assertDistributionsMergeCount(contentTwo.copy(), contentOne.copy(), 1, false);

        Optional<Identified> optionalContentOne = Optional.of(contentOne.copy());
        Content mergeResult = contentWriteMerger.merge(optionalContentOne, contentTwo.copy(), true);

        assertEquals(ImmutableSet.copyOf(Iterables.concat(
                contentTwo.getDistributions(),
                contentOne.getDistributions()
                )).asList(), mergeResult.getDistributions()
        );
    }

    @Test
    public void languageMergingTest() {
        Item contentOne = makeItem();
        Item contentTwo = makeItem();

        assertLanguageMergeCount(contentOne.copy(), contentTwo.copy(), false, true);
        assertLanguageMergeCount(contentTwo.copy(), contentOne.copy(), false, false);

        contentOne.setLanguage(Language.builder()
                .withCode("code")
                .withDisplay("display")
                .withDubbing("dubbing")
                .build()
        );

        assertLanguageMergeCount(contentOne.copy(), contentTwo.copy(), true, true);
        assertLanguageMergeCount(contentTwo.copy(), contentOne.copy(), true, false);

        contentTwo.setLanguage(Language.builder()
                .withCode("code 2")
                .withDisplay("display 2")
                .withDubbing("dubbing 2")
                .build()
        );

        assertLanguageMergeCount(contentOne.copy(), contentTwo.copy(), true, true);
        assertLanguageMergeCount(contentTwo.copy(), contentOne.copy(), true, false);
    }


    private void assertDistributionsMergeCount(
            Item contentOne,
            Item contentTwo,
            long expected,
            boolean merge
    ) {
        Optional<Identified> optionalContentOne = Optional.of(contentOne);
        Content mergeResultOne = contentWriteMerger.merge(optionalContentOne, contentTwo, merge);

        assertEquals(expected, mergeResultOne.getDistributions().spliterator().getExactSizeIfKnown());
    }

    private List<Review> makeReviews(
            String locale,
            String reviewString,
            String authorInitials,
            String authorName
    ) {
        Review review = new Review(new Locale(locale), reviewString);
        review.setAuthor(Author.builder()
                .withAuthorInitials(authorInitials)
                .withAuthorName(authorName)
                .build()
        );
        review.setType("type");
        List<Review> reviewList = new ArrayList<>();
        reviewList.add(review);
        return reviewList;
    }

    private Set<LocalizedTitle> makeLocalizedTitles(
            String type,
            String locale,
            String title
    ) {
        LocalizedTitle localizedTitle = new LocalizedTitle();
        localizedTitle.setType(type);
        localizedTitle.setLocale(new Locale(locale));
        localizedTitle.setTitle(title);

        Set<LocalizedTitle> localizedTitleSet = new HashSet<>();
        localizedTitleSet.add(localizedTitle);

        return localizedTitleSet;
    }

    private void assertLocalizedTitleMergeCount(
            Item contentOne,
            Item contentTwo,
            int expected,
            boolean merge
    ) {
        Optional<Identified> optionalContentOne = Optional.of(contentOne);
        Content mergeResult = contentWriteMerger.merge(optionalContentOne, contentTwo, merge);

        assertEquals(expected, mergeResult.getLocalizedTitles().size());
    }

    private void assertReviewMergeCount(
            Item contentOne,
            Item contentTwo,
            int mergeExpected,
            boolean merge
    ) {
        Optional<Identified> optionalContentOne = Optional.of(contentOne);
        Content mergeResultOne = contentWriteMerger.merge(optionalContentOne, contentTwo, merge);

        assertEquals(mergeExpected, mergeResultOne.getReviews().size());
    }

    private void assertLanguageMergeCount(
            Item contentOne,
            Item contentTwo,
            boolean mergeExpected,
            boolean merge
    ) {
        Optional<Identified> optionalContentOne = Optional.of(contentOne);
        Content mergeResultOne = contentWriteMerger.merge(optionalContentOne, contentTwo, merge);

        assertEquals(mergeExpected, mergeResultOne.getLanguage() != null);
    }

    private Item makeItem() {
        Item item = new Item();
        item.setCanonicalUri("uri");
        item.setImages(ImmutableSet.of());

        return item;
    }
}