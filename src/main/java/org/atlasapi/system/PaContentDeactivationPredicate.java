package org.atlasapi.system;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.BloomFilter;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Series;


import static com.google.common.base.Preconditions.checkNotNull;

public class PaContentDeactivationPredicate implements Predicate<Content> {

    private static final Predicate<String> HAS_FILM_URI = new Predicate<String>() {
        @Override
        public boolean apply(String s) {
            return !Strings.isNullOrEmpty(s) && s.startsWith("http://pressassociation.com/films/");
        }
    };

    private final BloomFilter<Long> activeContentIds;

    public PaContentDeactivationPredicate(BloomFilter<Long> activeContentIds) {
        this.activeContentIds = checkNotNull(activeContentIds);
    }

    @Override
    public boolean apply(Content content) {
        return (isNotPaFilm(content) &&
                isNotActiveContentId(content) &&
                isNotGenericDescription(content) &&
                isNotSeries(content)) ||
                isEmptyContainer(content);
    }

    private boolean isNotSeries(Content content) {
        return !(content instanceof Series);
    }

    private boolean isNotGenericDescription(Content content) {
        return content.getGenericDescription() != null &&
                !content.getGenericDescription();
    }

    private boolean isNotActiveContentId(Content content) {
        return !activeContentIds.mightContain(content.getId());
    }

    private boolean isNotPaFilm(Content content) {
        return !Iterables.any(content.getAllUris(), HAS_FILM_URI);
    }

    private boolean isEmptyContainer(Content content) {
        if (!(content instanceof Container)) {
            return false;
        }
        if (content instanceof Series) {
            Series series = (Series) content;
            return series.getChildRefs().isEmpty();
        }
        if (content instanceof Brand) {
            Brand brand = (Brand) content;
            return brand.getChildRefs().isEmpty() && brand.getSeriesRefs().isEmpty();
        }
        return false;
    }
}