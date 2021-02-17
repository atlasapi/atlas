package org.atlasapi.equiv.scorers.aenetworks;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
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
import org.atlasapi.media.entity.*;
import org.atlasapi.persistence.content.ContentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.utils.barb.BarbEquivUtils.TXLOG_PUBLISHERS;

/**
 * Caveat: Txlog names are scored against a suggestion's brand title and series title as well,
 * along with their permutations so do not rely too heavily on this score for equiv.
 * This score should be used as a guideline to ensure two unrelated pieces of content don't equiv purely
 * on a single other factor such as broadcast times for Txlogs.
 */
public class AeTitleMatchingItemScorer implements EquivalenceScorer<Item> {
    private static final Logger log = LoggerFactory.getLogger(AeTitleMatchingItemScorer.class);

    public static final String NAME = "A+E-Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of(
            "film:", "new:", "live:"
    );
    private static final Pattern TRAILING_YEAR_PATTERN = Pattern.compile("^(.*)\\(\\d{4}\\)$");
    private static final Pattern GENERIC_TITLE_PATTERN = Pattern.compile("^(([Ss]eries)|([Ee]pisode)) \\d+$");
    private static final Pattern SERIES_IN_TITLE_PATTERN = Pattern.compile("(series \\d+)");
    private static final Pattern EPISODE_IN_TITLE_PATTERN = Pattern.compile("(episode \\d+)");

    private static final Joiner TITLE_PERMUTATION_JOINER = Joiner.on('-').skipNulls();

    private static final Pattern TRAILING_APOSTROPHE_PATTERN = Pattern.compile("\\w' ");
    private static final Score DEFAULT_SCORE_ON_PERFECT_MATCH = Score.valueOf(2D);
    private static final Score DEFAULT_SCORE_ON_PARTIAL_MATCH = Score.ONE;
    private static final Score DEFAULT_SCORE_ON_MISMATCH = Score.ZERO;
    private static final int DEFAULT_CONTAINER_CACHE_DURATION = 60;
    private static final int TXLOG_TITLE_LENGTH = 40;

    private static final String AE_NETWORKS_SERIES_TITLE_CUSTOM_FIELD_NAME = "ae:series_title";
    private static final String AE_NETWORKS_EPISODE_NUMBER_FIELD_NAME = "ae:episode_number";
    private static final String TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME = "txlog:episode_title";
    //AE Networks<->Txlog specific rules
    //Lowercase because the logic happens after converting content titles to lower case
    private static final Pattern SERIES_AND_EPISODE_PATTERN = Pattern.compile(".*\\d* - \\d* -(.+)");
    private static final Pattern THE_AT_THE_END_PATTERN = Pattern.compile("(,\\s+the)");
    private static final Pattern PART_PATTERN = Pattern.compile("(part\\s*\\d+)");

    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();
    private final Score scoreOnPerfectMatch;
    private final Score scoreOnPartialMatch;
    private final Score scoreOnMismatch;
    @Nullable
    private final ContentResolver contentResolver;

    public enum TitleType {

        DATE("\\d{1,2}/\\d{1,2}/(\\d{2}|\\d{4})"),
        SEQUENCE("((?:E|e)pisode)(?:.*)(\\d+)"),
        DEFAULT(".*");

        private final Pattern pattern;

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

    private AeTitleMatchingItemScorer(Builder builder) {
        scoreOnPerfectMatch = checkNotNull(builder.scoreOnPerfectMatch);
        scoreOnPartialMatch = checkNotNull(builder.scoreOnPartialMatch);
        scoreOnMismatch = checkNotNull(builder.scoreOnMismatch);
        contentResolver = builder.contentResolver;
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
        scorerComponent.setComponentName("A+E Title Matching Item Scorer");

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

    public Score score(Item subject, Item suggestion, ResultDescription desc) {
        // txlog titles can often contain brand or series information so score the txlog title
        // against these titles as well if applicable
        Score equivScore = scoreOnMismatch;
        Optional<Score> parentScore = scoreWithParentInformation(subject, suggestion, desc, false);
        if (parentScore.isPresent() && parentScore.get().isRealScore()) {
            equivScore = parentScore.get();
        }
        desc.finishStage();
        if (equivScore != scoreOnPerfectMatch) {
            desc.startStage(
                    String.format(
                            "Scoring suggestion episode title: %s (%s)",
                            suggestion.getCustomField(TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME),
                            suggestion.getCanonicalUri()
                    )
            );
            Optional<Score> aeParentScore = scoreWithParentInformation(subject, suggestion, desc, true);
            if (aeParentScore.isPresent() && aeParentScore.get().isRealScore()
                    && (!equivScore.isRealScore() || aeParentScore.get().asDouble() > equivScore.asDouble())) {
                equivScore = aeParentScore.get();
            }
            desc.finishStage();
        }
        return equivScore;
    }

    private Optional<Score> scoreWithParentInformation(Item subject, Item suggestion, ResultDescription desc, boolean useTxlogEpisodeTitle) {
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

        ContentTitleMatchingFields txlogItemFields;
        String txlogTitleClarification = "";
        if (useTxlogEpisodeTitle) {
            Item txlogItemWithEpisodeTitle = txlogItem.copy();
            String episodeTitle = txlogItem.getCustomField(TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME);
            txlogItemWithEpisodeTitle.setTitle(episodeTitle);
            txlogItemFields = new ContentTitleMatchingFields(txlogItemWithEpisodeTitle);
            txlogTitleClarification = "episode";
        } else {
            txlogItemFields = new ContentTitleMatchingFields(txlogItem);
            txlogTitleClarification = "series";
        }
        ContentTitleMatchingFields nonTxlogItemFields = new ContentTitleMatchingFields(nonTxlogItem);

        Set<ContentTitleMatchingFields> nonTxlogAndParentFields = new HashSet<>(3);
        nonTxlogAndParentFields.add(nonTxlogItemFields);
        String seriesTitle = nonTxlogItem.getCustomField(AE_NETWORKS_SERIES_TITLE_CUSTOM_FIELD_NAME);
        nonTxlogAndParentFields.add(nonTxlogItemFields.withTitle(seriesTitle));


        if (nonTxlogAndParentFields.size() <= 1) {
            return Optional.empty();
        }

        Set<Set<ContentTitleMatchingFields>> nonTxlogFieldsPowerSet = Sets.powerSet(nonTxlogAndParentFields);

        desc.startStage("Scoring permutations for " + nonTxlogItem.getCanonicalUri());
        Optional<Score> bestScore = scorePermutations(nonTxlogFieldsPowerSet, nonTxlogItemFields, nonTxlogItem.getCustomFields(), txlogItemFields, txlogItem.getCustomFields(), txlogTitleClarification, desc);
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
            Map<String, String> nonTxlogCustomFields, ContentTitleMatchingFields txlogItemFields,
            Map<String, String> txlogCustomFields, String txlogTitleClarification, ResultDescription desc
    ) {
        Score bestScore = null;
        for (Set<ContentTitleMatchingFields> nonTxlogFieldsSubset : nonTxlogFieldsPowerSet) {
            if (nonTxlogFieldsSubset.isEmpty()) {
                continue;
            }
            Collection<List<ContentTitleMatchingFields>> nonTxlogFieldsPermutations =
                    Collections2.permutations(nonTxlogFieldsSubset);

            for (List<ContentTitleMatchingFields> nonTxlogFieldsPermutation : nonTxlogFieldsPermutations) {
                List<String> titles = nonTxlogFieldsPermutation.stream()
                        .map(ContentTitleMatchingFields::getTitle)
                        .filter(Objects::nonNull)
                        .collect(MoreCollectors.toImmutableList());

                if (titles.isEmpty()) {
                    continue;
                }

                if (titles.size() == 1) {
                    // Ignore cases where one of the permutations is just "Series X" or "Episode X" just in case these
                    // could cause some false positives.
                    Matcher genericTitlePatternMatcher = GENERIC_TITLE_PATTERN.matcher(Iterables.getOnlyElement(titles));
                    if (genericTitlePatternMatcher.matches()) {
                        continue;
                    }
                }

                String concatenatedTitle = TITLE_PERMUTATION_JOINER.join(titles);

                // Copy the existing item fields object with a new title as if it were the title of the item.
                // This is so we can still keep some information from the item such as year in case it is useful.
                ContentTitleMatchingFields permutationFields = nonTxlogItemFields.withTitle(concatenatedTitle);
                Score permutationScore = scoreContent(txlogItemFields, txlogCustomFields, permutationFields, nonTxlogCustomFields, txlogTitleClarification, desc);
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
            Map<String, String> subjectCustomFields, ContentTitleMatchingFields suggestion,
            Map<String, String> suggestionCustomFields, String txlogTitleClarification, ResultDescription desc
    ) {
        Score score = Score.nullScore();
        if (!Strings.isNullOrEmpty(subject.getTitle())) {
            if (Strings.isNullOrEmpty(suggestion.getTitle())) {
                desc.appendText("No Title so scored %s", score);
            } else {
                score = processAndScoreContent(subject, subjectCustomFields, suggestion, suggestionCustomFields, txlogTitleClarification, desc);
            }
        }
        return score;
    }


    private Score processAndScoreContent(ContentTitleMatchingFields subject, Map<String, String> subjectCustomFields, ContentTitleMatchingFields suggestion, Map<String, String> suggestionCustomFields, String txlogTitleClarification, ResultDescription desc) {
        String subjectTitle = subject.getTitle().trim().toLowerCase();
        String suggestionTitle = suggestion.getTitle().trim().toLowerCase();
        String processedSubjectTitle = null;
        String processedSuggestionTitle = null;

        //TxLog titles are size capped, so truncate everything if we are equiving to txlogs
        if (TXLOG_PUBLISHERS.contains(suggestion.getPublisher())
                || (TXLOG_PUBLISHERS.contains(subject.getPublisher()))) {

            if (suggestion.getPublisher().equals(Publisher.AE_NETWORKS)
                    || subject.getPublisher().equals(Publisher.AE_NETWORKS)
            ) {
                processedSubjectTitle = processTitle(subjectTitle, subjectCustomFields);
                processedSuggestionTitle = processTitle(suggestionTitle, suggestionCustomFields);
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
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", scoreOnPerfectMatch, processedSuggestionTitle, txlogTitleClarification, processedSubjectTitle);
            return scoreOnPerfectMatch;
        }

        TitleType subjectType = TitleType.titleTypeOf(subject.getTitle());
        TitleType suggestionType = TitleType.titleTypeOf(suggestion.getTitle());

        if (subjectType == suggestionType) {
            return compareTitles(subjectTitle, suggestionTitle, txlogTitleClarification, desc);
        }

        desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", scoreOnMismatch, processedSuggestionTitle, txlogTitleClarification, processedSubjectTitle);
        return Score.nullScore();
    }

    private String removeArbitraryTrailingYear(String suggestionTitle) {
        Matcher matcher = TRAILING_YEAR_PATTERN.matcher(suggestionTitle);
        suggestionTitle = matcher.matches()
                ? matcher.group(1)
                : suggestionTitle;
        return suggestionTitle;
    }

    private Score compareTitles(final String subjectTitle, final String suggestionTitle, String txlogTitleClarification, ResultDescription desc) {
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
            return partialTitleScore(subjectTitle, suggestionTitle, txlogTitleClarification, desc);
        } else {
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", scoreOnPerfectMatch, suggTitle, txlogTitleClarification, subjTitle);
            return scoreOnPerfectMatch;
        }
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

    private String regularExpressionReplaceSpecialChars(String title) {
        return applyCommonReplaceRules(title)
                .replaceAll("[^A-Za-z0-9\\s']+", "-")
                .replace(" ", "\\-")
                .replaceAll("'\\\\-", "(\\\\w+|\\\\W*)\\-");
    }

    private Score partialTitleScore(String subjectTitle, String suggestionTitle, String txlogTitleClarification, ResultDescription desc) {

        String subjectTitleWithoutLeadingColons = removeLeadingColons(subjectTitle);
        String suggestionTitleWithoutLeadingColons = removeLeadingColons(suggestionTitle);

        String subjTitle = normalizeWithoutReplacing(subjectTitleWithoutLeadingColons);
        String suggTitle = normalizeWithoutReplacing(suggestionTitleWithoutLeadingColons);

        if (subjTitle.contains(":") && suggTitle.contains(":")) {
            subjTitle = subjTitle.substring(0, subjTitle.indexOf(":"));
            suggTitle = suggTitle.substring(0, suggTitle.indexOf(":"));
            Score finalScore = subjTitle.equals(suggTitle) ? scoreOnPartialMatch : scoreOnMismatch;
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", finalScore, suggTitle, txlogTitleClarification, subjTitle);
            return finalScore;
        } else if (subjTitle.contains(":") && subjTitle.length() > suggTitle.length()) {
            String subjSubstring = subjTitle.substring(0, subjTitle.indexOf(":"));
            Score finalScore = subjSubstring.equals(suggTitle) ? scoreOnPartialMatch : scoreOnMismatch;
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", finalScore, suggTitle, txlogTitleClarification, subjTitle);
            return finalScore;
        } else if (suggTitle.contains(":")) {
            String suggSubstring = suggTitle.substring(0, suggestionTitle.indexOf(":"));
            Score finalScore = suggSubstring.equals(subjTitle) ? scoreOnPartialMatch : scoreOnMismatch;
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", finalScore, suggTitle, txlogTitleClarification, subjTitle);
            return finalScore;
        } else if((subjTitle.startsWith(suggTitle) && !suggTitle.isEmpty()) || (suggTitle.startsWith(subjTitle) && !suggTitle.isEmpty()))
        {
            desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", scoreOnPartialMatch, suggTitle, txlogTitleClarification, subjTitle);
            return scoreOnPartialMatch;
        }
        desc.appendText("%s: A+E processed title: %s, Txlog processed %s title: %s", scoreOnMismatch, suggTitle, txlogTitleClarification, subjTitle);
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
     * Used for custom rules between txlogs and AE Networks
     */
    private String processTitle(String title, Map<String, String> customFields) {
        title = title.replaceAll("\\s\\s+", " ");
        Matcher seriesEpisodeMatcher = SERIES_AND_EPISODE_PATTERN.matcher(title);
        if (seriesEpisodeMatcher.find()) {
            title = seriesEpisodeMatcher.group(1);
        }
        //Removes # number
        title = title.replaceAll("#\\s*\\w+", "");
        //Removes (number)
        title = title.replaceAll("\\(\\d+\\)", "");
        //Removes <copy>
        title = title.replaceAll("<copy>", "");
        //Places the word 'the' at the start
        Matcher theAtTheEndMatcher = THE_AT_THE_END_PATTERN.matcher(title);
        if (theAtTheEndMatcher.find()) {
            title = String.format("the %s", title.trim().replaceAll(theAtTheEndMatcher.group(1), "").trim());
        }
        //Removes Series #
        Matcher seriesInTitleMatcher = SERIES_IN_TITLE_PATTERN.matcher(title);
        if (seriesInTitleMatcher.find()) {
            title = title.trim().replaceAll(seriesInTitleMatcher.group(1), "").trim();
        }

        //Removes Episode #
        Matcher episodeInTitleMatcher = EPISODE_IN_TITLE_PATTERN.matcher(title);
        if (episodeInTitleMatcher.find()) {
            title = title.trim().replaceAll(episodeInTitleMatcher.group(1), "").trim();
        }

        //Removes special characters
        title = title.replaceAll("[^a-z0-9 :]", "");

        //Checks if contains 'part #'
        Matcher partMatcher = PART_PATTERN.matcher(title);
        String partNumber = "";
        if (partMatcher.find()) {
            partNumber = partMatcher.group(1);
            title = title.trim().replaceAll(partMatcher.group(1), "").trim();
        }

        if (customFields.containsKey(AE_NETWORKS_EPISODE_NUMBER_FIELD_NAME)) {
            String episodeNumber = customFields.get(AE_NETWORKS_EPISODE_NUMBER_FIELD_NAME);
            //Removes episode number from title
            title = title.replaceAll(episodeNumber, "").trim();
        }

        //Adds 'part #'
        title = title + " " + partNumber;

        //Removes more than one spaces
        title = title.replaceAll("\\s\\s+", " ");
        return title.trim();
    }

    @Override
    public String toString() {
        return "A+E Title-matching Item Scorer";
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

        public AeTitleMatchingItemScorer build() {
            return new AeTitleMatchingItemScorer(this);
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
