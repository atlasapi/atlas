package org.atlasapi.deer.elasticsearch;

import com.google.common.collect.Iterables;
import org.atlasapi.deer.elasticsearch.content.Specialization;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;

// I have simplified this class to avoid importing other classes we don't need
public class FiltersBuilder {

    private FiltersBuilder() {
    }

    public static TermsFilterBuilder buildForPublishers(
            String field,
            Iterable<Publisher> publishers
    ) {
        return FilterBuilders.termsFilter(field, Iterables.transform(publishers, Publisher.TO_KEY));
    }

    public static TermsFilterBuilder buildForSpecializations(
            Iterable<Specialization> specializations
    ) {
        return FilterBuilders.termsFilter(
                EsContent.SPECIALIZATION,
                Iterables.transform(specializations, Enum::name)
        );
    }
}
