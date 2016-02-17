package org.atlasapi.system;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;


import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PaContentDeactivationPredicate implements Predicate<Content> {

    private static final Pattern ID_EXTRACTION_PATTERN = Pattern.compile(".*/([0-9]+)");

    private static final Function<String, String> PA_ALIAS_EXTRACTOR = new Function<String, String>() {
        @Override public String apply(String s) {
            Matcher matcher = ID_EXTRACTION_PATTERN.matcher(s);
            checkArgument(matcher.matches(), "Not a PA alias");
            return matcher.group(1);
        }
    };

    private static final Predicate<String> HAS_FILM_URI = new Predicate<String>() {
        @Override public boolean apply(String s) {
            return !Strings.isNullOrEmpty(s) && s.startsWith("http://pressassociation.com/films/");
        }
    };

    private static final Predicate<String> IS_PA_ALIAS = new Predicate<String>() {
        @Override public boolean apply(@Nullable String s) {
            return !Strings.isNullOrEmpty(s) &&
                    (s.startsWith("http://pressassociation.com/episodes") ||
                    s.startsWith("http://pressassociation.com/brands") ||
                    s.startsWith("http://pressassociation.com/series"));
        }
    };

    private final Multimap<String, String> paNamespaceToAliases;

    public PaContentDeactivationPredicate(Multimap<String, String> paNamespaceToAliases) {
        this.paNamespaceToAliases = checkNotNull(paNamespaceToAliases);
    }

    /**
     * Returns true if content should be deactivated.
     */
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
        return content.getGenericDescription() == null || !content.getGenericDescription();
    }

    private boolean isNotActiveContentId(final Content content) {
        return FluentIterable.from(content.getAllUris())
                .filter(IS_PA_ALIAS)
                .transform(PA_ALIAS_EXTRACTOR)
                .anyMatch(shouldDeactivateContent(content));
    }

    private Predicate<String> shouldDeactivateContent(final Content content) {
        return new Predicate<String>() {
            @Override public boolean apply(@Nullable String s) {
                if (content instanceof Episode || content instanceof Item) {
                    return paNamespaceToAliases
                            .get(PaContentDeactivator.PA_PROGRAMME_NAMESPACE)
                            .contains(s);
                }
                if (content instanceof Series) {
                    return paNamespaceToAliases
                            .get(PaContentDeactivator.PA_SEASON_NAMESPACE)
                            .contains(s);
                }
                if (content instanceof Brand) {
                    return paNamespaceToAliases
                            .get(PaContentDeactivator.PA_SERIES_NAMESPACE)
                            .contains(s);
                }
                return false;
            }
        };
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