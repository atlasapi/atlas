package org.atlasapi.remotesite.pa.deletes;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.atlasapi.media.entity.*;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PaContentDeactivationPredicate implements Predicate<Content> {

    private static final Pattern ID_EXTRACTION_PATTERN = Pattern.compile(".*/([0-9]+)");

    private static final Function<String, String> PA_ALIAS_EXTRACTOR = new Function<String, String>() {
        @Override
        public String apply(String s) {
            Matcher matcher = ID_EXTRACTION_PATTERN.matcher(s);
            if (!matcher.matches()) {
                return null;
            }
            return matcher.group(1);
        }
    };

    private static final Predicate<String> HAS_FILM_URI = new Predicate<String>() {
        @Override
        public boolean apply(String s) {
            return !Strings.isNullOrEmpty(s) && s.startsWith("http://pressassociation.com/films/");
        }
    };

    private static final Predicate<String> IS_PA_ALIAS = new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            return !Strings.isNullOrEmpty(s) &&
                    (s.startsWith("http://pressassociation.com/episodes") ||
                            s.startsWith("http://pressassociation.com/brands") ||
                            s.startsWith("http://pressassociation.com/series"));
        }
    };
    private static final Predicate<String> IS_GENERIC_ID = new Predicate<String>() {
        @Override
        public boolean apply(String s) {
            /* Generics consistently have PA IDs greater than 100 million */
            return Long.parseLong(s) > 100000000;
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
                isInactiveContent(content) &&
                isNotGenericDescription(content) &&
                isNotSeries(content)) ||
                isEmptyContainer(content);
    }

    private boolean isNotSeries(Content content) {
        return !(content instanceof Series);
    }

    private boolean isNotGenericDescription(Content content) {
        /* We dont use Content.getGenericDescription here as some generic content predates that field */
        boolean hasGenericId = FluentIterable.from(content.getAllUris())
                .filter(IS_PA_ALIAS)
                .transform(PA_ALIAS_EXTRACTOR)
                .anyMatch(IS_GENERIC_ID);
        boolean hasGenericFlag = content.getGenericDescription() != null && content.getGenericDescription();
        return !hasGenericId && !hasGenericFlag;

    }

    private boolean isInactiveContent(final Content content) {
        return FluentIterable.from(content.getAllUris())
                .filter(IS_PA_ALIAS)
                .transform(PA_ALIAS_EXTRACTOR)
                .anyMatch(shouldDeactivateContent(content));
    }

    private Predicate<String> shouldDeactivateContent(final Content content) {
        return new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String s) {
                if (Strings.isNullOrEmpty(s)) {
                    return false;
                }
                /* Series aren't handled here as we lack a reliable mapping from PA's IDs
                    to their URIs in atlas. They are handled by removing empty Series elsewhere */
                if (content instanceof Episode || content instanceof Item) {
                    return !paNamespaceToAliases
                            .get(PaContentDeactivator.PA_PROGRAMME_NAMESPACE)
                            .contains(s);
                }
                if (content instanceof Brand) {
                    return !paNamespaceToAliases
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