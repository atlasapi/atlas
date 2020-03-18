package org.atlasapi.equiv.scorers.barb;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.apache.commons.lang3.StringUtils;
import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.utils.barb.BarbEquivUtils.TXLOG_PUBLISHERS;

/**
 * Caveat: Txlog names are scored against a suggestion's brand title and series title as well,
 * along with their permutations so do not rely to heavily on this score for equiv.
 * This score should be used as a guideline to ensure two unrelated pieces of content don't equiv purely
 * on a single other factor such as broadcast times for Txlogs.
 */
public class BarbTitleMatchingItemScorer implements EquivalenceScorer<Item> {
    private static final Logger log = LoggerFactory.getLogger(BarbTitleMatchingItemScorer.class);

    public static final String NAME = "Barb-Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of(
            "the ", "live ", "film:", "new:", "live:"
    );
    private static final ImmutableSet<String> POSTFIXES = ImmutableSet.of("\\(unrated\\)", "\\(rated\\)");
    private static final Pattern TRAILING_YEAR_PATTERN = Pattern.compile("^(.*)\\(\\d{4}\\)$");
    private static final Pattern GENERIC_TITLE_PATTERN = Pattern.compile("^(([Ss]eries)|([Ee]pisode)) \\d+$");

    private static final Joiner TITLE_PERMUTATION_JOINER = Joiner.on('-').skipNulls();

    private static final Pattern TRAILING_APOSTROPHE_PATTERN = Pattern.compile("\\w' ");
    private static final Score DEFAULT_SCORE_ON_PERFECT_MATCH = Score.valueOf(2D);
    private static final Score DEFAULT_SCORE_ON_PARTIAL_MATCH = Score.ONE;
    private static final Score DEFAULT_SCORE_ON_MISMATCH = Score.ZERO;
    private static final int DEFAULT_CONTAINER_CACHE_DURATION = 60;
    private static final int TXLOG_TITLE_LENGTH = 40;

    //Nitro<->Txlog specific rules
    //Lowercase because the logic happens after converting content titles to lower case
    private static final Pattern BBC_O_CLOCK_NEWS_PATTERN = Pattern.compile("(\\w+) o'clock news");
    private static final String BBC_NEWS_AT_O_CLOCK_REPLACEMENT = "news at ";
    private static final Pattern BBC_WEEKEND_NEWS_PATTERN = Pattern.compile("weekend news");
    private static final String BBC_WEEKEND_NEWS_REPLACEMENT = "news";
    private static final ImmutableMap<String, String> BBC_TITLE_REPLACEMENTS = ImmutableMap.of(
            "news 24", "joins bbc news",
            "!mpossible", "impossible"
    );
    private static final ImmutableSet<String> BBC_PREFIXES = ImmutableSet.of(
            "bbc"
    );
    private static final ImmutableSet<String> BBC_SUFFIXES = ImmutableSet.of(
            "highlights"
    );

    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();
    private final Score scoreOnPerfectMatch;
    private final Score scoreOnPartialMatch;
    private final Score scoreOnMismatch;
    @Nullable private final ContentResolver contentResolver;
    private final LoadingCache<String, Optional<ContentTitleMatchingFields>> topLevelContainerCache;
    private final LoadingCache<String, Optional<ContentTitleMatchingFields>> seriesCache;

    public enum TitleType {

        DATE("\\d{1,2}/\\d{1,2}/(\\d{2}|\\d{4})"),
        SEQUENCE("((?:E|e)pisode)(?:.*)(\\d+)"),
        DEFAULT(".*");

        private Pattern pattern;

        TitleType(String pattern) {
            this.pattern = Pattern.compile(pattern);
        }

        public static TitleType titleTypeOf(Item item) {
            return titleTypeOf(item.getTitle());
        }

        public static TitleType titleTypeOf(String title) {
            for (TitleType type : ImmutableList.copyOf(TitleType.values())) {
                if (type.matches(title)) {
                    return type;
                }
            }
            return DEFAULT;
        }


        private boolean matches(String title) {
            return pattern.matcher(title).matches();
        }

    }

    private BarbTitleMatchingItemScorer(Builder builder) {
        scoreOnPerfectMatch = checkNotNull(builder.scoreOnPerfectMatch);
        scoreOnPartialMatch = checkNotNull(builder.scoreOnPartialMatch);
        scoreOnMismatch = checkNotNull(builder.scoreOnMismatch);
        contentResolver = builder.contentResolver;
        topLevelContainerCache = CacheBuilder.newBuilder()
                .expireAfterWrite(checkNotNull(builder.containerCacheDuration), TimeUnit.SECONDS)
                .build(new CacheLoader<String, Optional<ContentTitleMatchingFields>>() {
                    @Override
                    public Optional<ContentTitleMatchingFields> load(@Nonnull String containerUri) {
                        return getContainer(containerUri);
                    }
                });
        seriesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(checkNotNull(builder.containerCacheDuration), TimeUnit.SECONDS)
                .build(new CacheLoader<String, Optional<ContentTitleMatchingFields>>() {
                    @Override
                    public Optional<ContentTitleMatchingFields> load(@Nonnull String seriesUri) {
                        return getContent(seriesUri);
                    }
                });
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> suggestions,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Barb Title Matching Item Scorer");

        DefaultScoredCandidates.Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        if (Strings.isNullOrEmpty(subject.getTitle())) {
            desc.appendText("No Title on subject, all score null");
        }

        for (Item suggestion : suggestions) {
            Score equivScore = score(subject, suggestion, desc);

            equivalents.addEquivalent(suggestion, equivScore);
            if (suggestion.getId() != null) {
                scorerComponent.addComponentResult(
                        suggestion.getId(),
                        String.valueOf(equivScore.asDouble())
                );
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Optional<ContentTitleMatchingFields> getContainer(String containerUri) {
        if (contentResolver == null) {
            return Optional.empty();
        }
        Maybe<Identified> resolved = contentResolver.findByCanonicalUris(
                ImmutableSet.of(containerUri)
        ).getFirstValue();
        if(resolved.hasValue()) {
            Container parent = (Container) resolved.requireValue();
            if (parent instanceof Series) { //this shouldn't be required but is being included as a sanity check
                Series series = (Series) parent;
                if (series.getParent() != null) {
                    resolved = contentResolver.findByCanonicalUris(
                            ImmutableSet.of(series.getParent().getUri()))
                            .getFirstValue();
                    if (resolved.hasValue()) {
                        parent = (Container) resolved.requireValue();
                    }
                }
            }
            return Optional.of(new ContentTitleMatchingFields(parent));
        }
        return Optional.empty();
    }

    private Optional<ContentTitleMatchingFields> getContent(String uri) {
        if (contentResolver == null) {
            return Optional.empty();
        }
        Maybe<Identified> resolved = contentResolver.findByCanonicalUris(
                ImmutableSet.of(uri)
        ).getFirstValue();
        if(resolved.hasValue()) {
            Content parent = (Content) resolved.requireValue();
            return Optional.of(new ContentTitleMatchingFields(parent));
        }
        return Optional.empty();
    }

    public Score score(Item subject, Item suggestion, ResultDescription desc) {
        desc.startStage(
                String.format(
                        "Scoring suggestion: %s (%s)",
                        suggestion.getTitle(),
                        suggestion.getCanonicalUri()
                )
        );
        Score equivScore = scoreContent(
                new ContentTitleMatchingFields(subject),
                new ContentTitleMatchingFields(suggestion),
                desc
        );
        if (equivScore != scoreOnPerfectMatch) {
            // txlog titles can often contain brand or series information so score the txlog title
            // against these titles as well if applicable
            Optional<Score> parentScore = scoreWithParentInformation(subject, suggestion, desc);
            if (parentScore.isPresent() && parentScore.get().isRealScore()
                    && (!equivScore.isRealScore() || parentScore.get().asDouble() > equivScore.asDouble())) {
                equivScore = parentScore.get();
            }
        }
        desc.finishStage();
        return equivScore;
    }

    private Optional<Score> scoreWithParentInformation(Item subject, Item suggestion, ResultDescription desc) {
        boolean singleTxlogPresent = TXLOG_PUBLISHERS.contains(subject.getPublisher())
                // xor since we need the other publisher to be different and have a parent
                ^ TXLOG_PUBLISHERS.contains(suggestion.getPublisher());

        if (contentResolver == null || !singleTxlogPresent) {
            return Optional.empty();
        }

        Item txlogItem;
        Item nonTxlogItem;

        if (TXLOG_PUBLISHERS.contains(subject.getPublisher())) {
            txlogItem = subject;
            nonTxlogItem = suggestion;
        } else {
            txlogItem = suggestion;
            nonTxlogItem = subject;
        }

        Optional<ContentTitleMatchingFields> nonTxlogParent = nonTxlogItem.getContainer() != null
                ? topLevelContainerCache.getUnchecked(nonTxlogItem.getContainer().getUri())
                : Optional.empty();

        if (!nonTxlogParent.isPresent()) {
            return Optional.empty();
        }

        Optional<ContentTitleMatchingFields> nonTxlogSeries = Optional.empty();

        if (nonTxlogItem instanceof Episode) {
            Episode nonTxlogEpisode = (Episode) nonTxlogItem;
            if (nonTxlogEpisode.getSeriesRef() != null) {
                nonTxlogSeries = seriesCache.getUnchecked(nonTxlogEpisode.getSeriesRef().getUri());
            }
        }

        ContentTitleMatchingFields txlogItemFields = new ContentTitleMatchingFields(txlogItem);
        ContentTitleMatchingFields nonTxlogItemFields = new ContentTitleMatchingFields(nonTxlogItem);

        Set<ContentTitleMatchingFields> nonTxlogAndParentFields = new HashSet<>(3);
        nonTxlogAndParentFields.add(nonTxlogItemFields);
        nonTxlogAndParentFields.add(nonTxlogParent.get());
        nonTxlogSeries.ifPresent(nonTxlogAndParentFields::add);

        Set<Set<ContentTitleMatchingFields>> nonTxlogFieldsPowerSet = Sets.powerSet(nonTxlogAndParentFields);

        desc.startStage("Scoring permutations for " + nonTxlogItem.getCanonicalUri());
        Optional<Score> bestScore = scorePermutations(nonTxlogFieldsPowerSet, nonTxlogItemFields, txlogItemFields, desc);
        desc.finishStage();

        return bestScore;
    }

    /**
     * Iterates through all possible combinations and permutations of the 3 non-txlog titles and compares the
     * concatenation to the txlog title.
     */
    private Optional<Score> scorePermutations(
            Set<Set<ContentTitleMatchingFields>> nonTxlogFieldsPowerSet,
            ContentTitleMatchingFields nonTxlogItemFields,
            ContentTitleMatchingFields txlogItemFields,
            ResultDescription desc
    ) {
        Score bestScore = null;
        for (Set<ContentTitleMatchingFields> nonTxlogFieldsSubset : nonTxlogFieldsPowerSet) {
            if (nonTxlogFieldsSubset.isEmpty()) {
                continue;
            }
            if (nonTxlogFieldsSubset.size() == 1) {
                ContentTitleMatchingFields onlyElement = Iterables.getOnlyElement(nonTxlogFieldsSubset);
                if (onlyElement == nonTxlogItemFields) { //this was checked earlier for efficiency
                    continue;
                }
                // Ignore cases where one of the permutations is just "Series X" or "Episode X" just in case these
                // could cause some false positives.
                Matcher genericTitlePatternMatcher = GENERIC_TITLE_PATTERN.matcher(onlyElement.getTitle());
                if (genericTitlePatternMatcher.matches()) {
                    continue;
                }
            }
            Collection<List<ContentTitleMatchingFields>> nonTxlogFieldsPermutations =
                    Collections2.permutations(nonTxlogFieldsSubset);

            for (List<ContentTitleMatchingFields> nonTxlogFieldsPermutation : nonTxlogFieldsPermutations) {
                String concatenatedTitle = TITLE_PERMUTATION_JOINER.join(
                        nonTxlogFieldsPermutation.stream()
                                .map(ContentTitleMatchingFields::getTitle)
                                .collect(MoreCollectors.toImmutableList())
                );
                // Copy the existing item fields object with a new title as if it were the title of the item.
                // This is so we can still keep some information from the item such as year in case it is useful.
                ContentTitleMatchingFields permutationFields = nonTxlogItemFields.withTitle(concatenatedTitle);
                Score permutationScore = scoreContent(txlogItemFields, permutationFields, desc);
                if (bestScore == null || !bestScore.isRealScore()
                        || (permutationScore.isRealScore() && permutationScore.asDouble() > bestScore.asDouble())) {
                    bestScore = permutationScore;
                    if (bestScore == scoreOnPerfectMatch) {
                        return Optional.of(bestScore);
                    }
                }
            }
        }
        return Optional.ofNullable(bestScore);
    }

    private Score scoreContent(
            ContentTitleMatchingFields subject,
            ContentTitleMatchingFields suggestion,
            ResultDescription desc
    ) {
        Score score = Score.nullScore();
        if (!Strings.isNullOrEmpty(subject.getTitle())) {
            if (Strings.isNullOrEmpty(suggestion.getTitle())) {
                desc.appendText("No Title so scored %s", score);
            } else {
                score = scoreContent(subject, suggestion);
                desc.appendText("%s: %s", score, suggestion.getTitle());
            }
        }
        return score;
    }


    private Score scoreContent(ContentTitleMatchingFields subject, ContentTitleMatchingFields suggestion) {
        String subjectTitle = subject.getTitle().trim().toLowerCase();
        String suggestionTitle = suggestion.getTitle().trim().toLowerCase();

        //TxLog titles are size capped, so truncate everything if we are equiving to txlogs
        if (TXLOG_PUBLISHERS.contains(suggestion.getPublisher())
            || (TXLOG_PUBLISHERS.contains(subject.getPublisher()))) {

            if (suggestion.getPublisher().equals(Publisher.BBC_NITRO)
                    || subject.getPublisher().equals(Publisher.BBC_NITRO)
            ) {
                String processedSubjectTitle = processBbcTitle(subjectTitle);
                String processedSuggestionTitle = processBbcTitle(suggestionTitle);
                if (!processedSubjectTitle.equals(subjectTitle) || !processedSuggestionTitle.equals(suggestionTitle)) {
                    subjectTitle = processedSubjectTitle;
                    suggestionTitle = processedSuggestionTitle;
                }
            }

            if (subjectTitle.length() > TXLOG_TITLE_LENGTH) {
                subjectTitle = subjectTitle.substring(0, TXLOG_TITLE_LENGTH);
            }
            if (suggestionTitle.length() > TXLOG_TITLE_LENGTH) {
                suggestionTitle = suggestionTitle.substring(0, TXLOG_TITLE_LENGTH);
            }
        }

        //if either subject or candidate is txlog, strip the year from the end
        if (TXLOG_PUBLISHERS.contains(suggestion.getPublisher())) {
            suggestionTitle = removeArbitraryTrailingYear(suggestionTitle);
        }
        if (TXLOG_PUBLISHERS.contains(subject.getPublisher())) {
            subjectTitle = removeArbitraryTrailingYear(subjectTitle);
        }

        //return early if you can.
        if (subjectTitle.equals(suggestionTitle)) {
            return scoreOnPerfectMatch;
        }

        TitleType subjectType = TitleType.titleTypeOf(subject.getTitle());
        TitleType suggestionType = TitleType.titleTypeOf(suggestion.getTitle());

        if (subjectType == suggestionType) {
            subjectTitle = removePostfix(subjectTitle, subject.getYear());
            suggestionTitle = removePostfix(suggestionTitle, suggestion.getYear());
            return compareTitles(subjectTitle, suggestionTitle);
        }

        return Score.nullScore();
    }

    private String removeArbitraryTrailingYear(String suggestionTitle) {
        Matcher matcher = TRAILING_YEAR_PATTERN.matcher(suggestionTitle);
        suggestionTitle = matcher.matches()
                ? matcher.group(1)
                : suggestionTitle;
        return suggestionTitle;
    }

    private String removePostfix(String title, @Nullable Integer year) {
        String removedYear = removeYearFromTitle(title, year);
        return removeRatings(removedYear).trim();
    }

    private String removeRatings(String title) {
        for (String postfix : POSTFIXES) {
            title = title.replaceAll(postfix, "");
        }
        return title;
    }

    private String removeYearFromTitle(String title, @Nullable Integer year) {

        if (year != null) {
            return title.replaceAll("\\(" + year + "\\)", "");
        } else {
            return title;
        }
    }

    private Score compareTitles(final String subjectTitle, final String suggestionTitle) {
        boolean matches;
        String subjTitle = normalize(subjectTitle);
        String suggTitle = normalize(suggestionTitle);

        if (appearsToBeWithApostrophe(subjectTitle)) {
            String regexp = normalizeRegularExpression(subjectTitle);
            matches = Pattern.matches(regexp, suggTitle);
        } else if (appearsToBeWithApostrophe(suggestionTitle)) {
            String regexp = normalizeRegularExpression(suggestionTitle);
            matches = Pattern.matches(regexp, subjTitle);
        } else {
            matches = matchWithoutDashes(subjTitle, suggTitle) || subjTitle.equals(suggTitle);
        }

        if (!matches) {
            return partialTitleScore(subjectTitle, suggestionTitle);
        } else {
            return scoreOnPerfectMatch;
        }
    }

    private Score partialTitleScore(String subjectTitle, String suggestionTitle) {

        String subjectTitleWithoutLeadingColons = removeLeadingColons(subjectTitle);
        String suggestionTitleWithoutLeadingColons = removeLeadingColons(suggestionTitle);

        String subjTitle = normalizeWithoutReplacing(subjectTitleWithoutLeadingColons);
        String suggTitle = normalizeWithoutReplacing(suggestionTitleWithoutLeadingColons);

        if (subjTitle.contains(":") && suggTitle.contains(":")) {

            subjTitle = subjTitle.substring(0, subjTitle.indexOf(":"));
            suggTitle = suggTitle.substring(0, suggTitle.indexOf(":"));
            return subjTitle.equals(suggTitle) ? scoreOnPartialMatch : scoreOnMismatch;
        } else if (subjTitle.contains(":") && subjTitle.length() > suggTitle.length()) {

            String subjSubstring = subjTitle.substring(0, subjTitle.indexOf(":"));
            return subjSubstring.equals(suggTitle) ? scoreOnPartialMatch : scoreOnMismatch;
        } else if (suggTitle.contains(":")) {

            String suggSubstring = suggTitle.substring(0, suggestionTitle.indexOf(":"));
            return suggSubstring.equals(subjTitle) ? scoreOnPartialMatch : scoreOnMismatch;
        }

        return scoreOnMismatch;
    }

    private String removeLeadingColons(String title) {
        Pattern colonsAtStart = Pattern.compile(":+(.*)");

        Matcher titleMatcher = colonsAtStart.matcher(title);

        return titleMatcher.matches() ? titleMatcher.group(1) : title;
    }

    private String normalize(String title) {
        String normalized = normalizeWithoutReplacing(title);
        return replaceSpecialChars(normalized);
    }

    private String normalizeWithoutReplacing(String title) {
        String withoutSequencePrefix = removeSequencePrefix(title).trim();
        String expandedTitle = titleExpander.expand(withoutSequencePrefix).trim();
        String withoutCommonPrefixes = removeCommonPrefixes(expandedTitle).trim();
        return StringUtils.stripAccents(withoutCommonPrefixes);
    }

    private String normalizeRegularExpression(String title) {
        return regularExpressionReplaceSpecialChars(removeCommonPrefixes(removeSequencePrefix(title)));
    }

    private boolean appearsToBeWithApostrophe(String title) {
        return TRAILING_APOSTROPHE_PATTERN.matcher(title).find();
    }

    private String applyCommonReplaceRules(String title) {
        return title
                .replaceAll(" vs. ", " vs ")
                .replaceAll(" v ", " vs ")
                .replaceAll(" & ", " and ")
                .replaceAll("fc ", "")
                .replaceAll(",", "");
    }

    private String replaceSpecialChars(String title) {
        return applyCommonReplaceRules(title)
                .replaceAll("\\.", "")
                .replaceAll("\\s?\\/\\s?", "-") // normalize spacing around back-to-back titles
                .replaceAll("[^A-Za-z0-9\\s']+", "-")
                .replace("'", "")
                .replace(" ", "-");

    }

    private String regularExpressionReplaceSpecialChars(String title) {
        return applyCommonReplaceRules(title)
                .replaceAll("[^A-Za-z0-9\\s']+", "-")
                .replace(" ", "\\-")
                .replaceAll("'\\\\-", "(\\\\w+|\\\\W*)\\-");
    }

    private String removeCommonPrefixes(String title) {
        return removePrefixes(title, PREFIXES);
    }

    private String removePrefixes(String title, Set<String> prefixes) {
        String remainingTitle = title;
        boolean removedAtLeastOne;
        do {
            removedAtLeastOne = false;
            for (String prefix : prefixes) {
                if (remainingTitle.length() > prefix.length() && remainingTitle.startsWith(prefix)) {
                    remainingTitle = remainingTitle.substring(prefix.length()).trim();
                    removedAtLeastOne = true;
                    break;
                }
            }
        }
        while (removedAtLeastOne);

        return remainingTitle;
    }

    private String removeSuffixes(String title, Set<String> suffixes) {
        String remainingTitle = title;
        boolean removedAtLeastOne;
        do {
            removedAtLeastOne = false;
            for (String suffix : suffixes) {
                if (remainingTitle.length() > suffix.length() && remainingTitle.endsWith(suffix)) {
                    remainingTitle = remainingTitle.substring(0, remainingTitle.length() - suffix.length()).trim();
                    removedAtLeastOne = true;
                    break;
                }
            }
        }
        while (removedAtLeastOne);

        return remainingTitle;
    }

    private boolean matchWithoutDashes(String subject, String suggestion) {
        return subject.replaceAll("-", "").equals(suggestion.replaceAll("-", ""));
    }

    //Matches e.g. "2. Kinross"
    private final Pattern seqTitle = Pattern.compile("\\s*\\d+\\s*[.:-]{1}\\s*(.*)");

    private String removeSequencePrefix(String title) {
        Matcher matcher = seqTitle.matcher(title);
        return matcher.matches() ? matcher.group(1) : title;
    }

    /**
     * Used for custom rules between BBC txlogs and Nitro
     */
    private String processBbcTitle(String title) {
        String replacement = BBC_TITLE_REPLACEMENTS.get(title);
        if (!Strings.isNullOrEmpty(replacement)) {
            return replacement;
        }
        title = removePrefixes(title, BBC_PREFIXES);
        title = removeSuffixes(title, BBC_SUFFIXES);
        Matcher oClockMatcher = BBC_O_CLOCK_NEWS_PATTERN.matcher(title);
        if (oClockMatcher.find()) {
            title = oClockMatcher.replaceFirst(BBC_NEWS_AT_O_CLOCK_REPLACEMENT + oClockMatcher.group(1));
        }
        title = BBC_WEEKEND_NEWS_PATTERN.matcher(title).replaceFirst(BBC_WEEKEND_NEWS_REPLACEMENT);
        return title;
    }

    @Override
    public String toString() {
        return "Barb Title-matching Item Scorer";
    }

    public static final class Builder {
        private Score scoreOnPerfectMatch = DEFAULT_SCORE_ON_PERFECT_MATCH;
        private Score scoreOnPartialMatch = DEFAULT_SCORE_ON_PARTIAL_MATCH;
        private Score scoreOnMismatch = DEFAULT_SCORE_ON_MISMATCH;
        private ContentResolver contentResolver;
        private int containerCacheDuration = DEFAULT_CONTAINER_CACHE_DURATION; //seconds

        private Builder() {
        }

        public Builder withScoreOnPerfectMatch(Score scoreOnPerfectMatch) {
            this.scoreOnPerfectMatch = scoreOnPerfectMatch;
            return this;
        }

        public Builder withScoreOnPartialMatch(Score scoreOnPartialMatch) {
            this.scoreOnPartialMatch = scoreOnPartialMatch;
            return this;
        }

        public Builder withScoreOnMismatch(Score scoreOnMismatch) {
            this.scoreOnMismatch = scoreOnMismatch;
            return this;
        }

        public Builder withContentResolver(ContentResolver contentResolver) {
            this.contentResolver = contentResolver;
            return this;
        }

        public Builder withContainerCacheDuration(int seconds) {
            this.containerCacheDuration = seconds;
            return this;
        }

        public BarbTitleMatchingItemScorer build() {
            return new BarbTitleMatchingItemScorer(this);
        }
    }


    // Mainly used so that we only cache necessary fields for containers
    private class ContentTitleMatchingFields {
        private final String canonicalUri;
        private final String title;
        private final Publisher publisher;
        private final Integer year;

        public ContentTitleMatchingFields(
                String canonicalUri,
                String title,
                Publisher publisher,
                Integer year
        ) {
            this.canonicalUri = canonicalUri;
            this.title = title;
            this.publisher = publisher;
            this.year = year;
        }

        public ContentTitleMatchingFields(Content content) {
            this(
                    content.getCanonicalUri(),
                    content.getTitle(),
                    content.getPublisher(),
                    content.getYear()
            );
        }

        public String getCanonicalUri() {
            return canonicalUri;
        }

        public String getTitle() {
            return title;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        public Integer getYear() {
            return year;
        }

        public ContentTitleMatchingFields withTitle(String title) {
            return new ContentTitleMatchingFields(
                    canonicalUri,
                    title,
                    publisher,
                    year
            );
        }
    }
}
