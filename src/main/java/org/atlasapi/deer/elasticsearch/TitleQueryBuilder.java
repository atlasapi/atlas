package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;

public class TitleQueryBuilder {

    private static final Joiner JOINER = Joiner.on("");
    private static final int USE_PREFIX_SEARCH_UP_TO = 2;
    private static final Map<String, String> EXPANSIONS = ImmutableMap.<String, String>builder().put(
            "dr",
            "doctor"
    ).put("rd", "road").build();
    private static final Function<String, String> TOKEN_EXPANDER = token -> {
        String expanded = EXPANSIONS.get(token);
        if (expanded != null) {
            return expanded;
        }
        return token;
    };

    public static QueryBuilder build(String title, float boost) {
        List<String> tokens = Strings.tokenize(title, true);
        QueryBuilder query = null;
        if (shouldUsePrefixSearch(tokens)) {
            query = prefixSearch(Iterables.getOnlyElement(tokens));
        } else {
            query = fuzzyTermSearch(Strings.flatten(title), tokens);
        }
        return QueryBuilders.functionScoreQuery(query).boost(boost);
    }

    private static boolean shouldUsePrefixSearch(List<String> tokens) {
        return tokens.size() == 1
                && Iterables.getOnlyElement(tokens).length() <= USE_PREFIX_SEARCH_UP_TO;
    }

    private static QueryBuilder prefixSearch(String token) {
        BoolQueryBuilder withExpansions = new BoolQueryBuilder();
        withExpansions.minimumNumberShouldMatch(1);
        withExpansions.should(prefixQuery(token));
        //
        String expanded = EXPANSIONS.get(token);
        if (expanded != null) {
            withExpansions.should(prefixQuery(expanded));
        }
        return withExpansions;
    }

    private static QueryBuilder prefixQuery(String prefix) {
        return new PrefixQueryBuilder(EsContent.PARENT_FLATTENED_TITLE, prefix);
    }

    private static QueryBuilder fuzzyTermSearch(String value, List<String> tokens) {
        BoolQueryBuilder queryForTerms = new BoolQueryBuilder();
        for (String token : tokens) {
            BoolQueryBuilder queryForThisTerm = new BoolQueryBuilder();
            queryForThisTerm.minimumNumberShouldMatch(1);

            QueryBuilder prefix = QueryBuilders.functionScoreQuery(new PrefixQueryBuilder(
                    EsContent.PARENT_TITLE,
                    token
            )).boost(50);
            queryForThisTerm.should(prefix);

            QueryBuilder fuzzy = new FuzzyQueryBuilder(EsContent.PARENT_TITLE, token)
                    .fuzziness(Fuzziness.AUTO)
                    .prefixLength(USE_PREFIX_SEARCH_UP_TO);
            queryForThisTerm.should(fuzzy);

            queryForTerms.must(queryForThisTerm);
        }

        BoolQueryBuilder either = new BoolQueryBuilder();
        either.minimumNumberShouldMatch(1);

        either.should(queryForTerms);
        either.should(fuzzyWithoutSpaces(value));

        QueryBuilder prefix = QueryBuilders.functionScoreQuery(prefixSearch(value)).boost(50);
        either.should(prefix);

        QueryBuilder exact = QueryBuilders.functionScoreQuery(exactMatch(value, tokens)).boost(200);
        either.should(exact);

        return either;
    }

    private static QueryBuilder exactMatch(String value, Iterable<String> tokens) {
        BoolQueryBuilder exactMatch = new BoolQueryBuilder();
        exactMatch.minimumNumberShouldMatch(1);
        exactMatch.should(new TermQueryBuilder(EsContent.PARENT_FLATTENED_TITLE, value));

        Iterable<String> transformed = Iterables.transform(tokens, TOKEN_EXPANDER);

        String flattenedAndExpanded = JOINER.join(transformed);

        if (!flattenedAndExpanded.equals(value)) {
            exactMatch.should(new TermQueryBuilder(
                    EsContent.PARENT_FLATTENED_TITLE,
                    flattenedAndExpanded
            ));
        }
        return exactMatch;
    }

    private static QueryBuilder fuzzyWithoutSpaces(String value) {
        return new FuzzyQueryBuilder(EsContent.PARENT_FLATTENED_TITLE, value)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(USE_PREFIX_SEARCH_UP_TO)
                .boost(20);
    }
}
