package org.atlasapi.equiv.scorers;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

/**
 * Caveat: Txlog names are scored against a suggestion's brand title as well so do not rely to heavily on this score for equiv
 * This score should be used as a guideline to ensure two unrelated pieces of content don't equiv purely on a single other factor
 * such as broadcast times for Txlogs
 */
public class BarbTitleMatchingItemScorer implements EquivalenceScorer<Item> {

    public static final String NAME = "Barb-Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of(
            "the ", "live ", "film:", "new:", "live:"
    );
    private static final ImmutableSet<String> POSTFIXES = ImmutableSet.of("\\(unrated\\)", "\\(rated\\)");
    private static final Pattern TRAILING_YEAR_PATTERN = Pattern.compile("^(.*)\\(\\d{4}\\)$");

    private static final Pattern TRAILING_APOSTROPHE_PATTERN = Pattern.compile("\\w' ");
    private static final Score DEFAULT_SCORE_ON_PERFECT_MATCH = Score.valueOf(2D);
    private static final Score DEFAULT_SCORE_ON_PARTIAL_MATCH = Score.ONE;
    private static final Score DEFAULT_SCORE_ON_MISMATCH = Score.ZERO;
    private static final int TXLOG_TITLE_LENGTH = 40;
    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();
    private final Score scoreOnPerfectMatch;
    private final Score scoreOnPartialMatch;
    private final Score scoreOnMismatch;
    @Nullable private final ContentResolver contentResolver;

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
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> suggestions,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Barb Title Matching Item Scorer");

        DefaultScoredCandidates.Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        if (Strings.isNullOrEmpty(subject.getTitle())) {
            desc.appendText("No Title on subject, all score null");
        }

        for (Item suggestion : suggestions) {
            Score equivScore = score(subject, suggestion, desc);
            if (equivScore != scoreOnPerfectMatch) {
                //txlog titles are often just the title of the brand so score against suggestion's brand title
                Optional<Score> parentScore = getParentScore(subject, desc, suggestion);
                if (parentScore.isPresent() && parentScore.get().isRealScore()
                    && (!equivScore.isRealScore() || parentScore.get().asDouble() > equivScore.asDouble())) {
                    desc.appendText(String.format(
                            "Parent for %s scored %.2f which is greater than its own score of %.2f",
                            suggestion.getCanonicalUri(),
                            parentScore.get().asDouble(),
                            equivScore.asDouble()
                    ));
                    equivScore = parentScore.get();
                }
            }

            equivalents.addEquivalent(suggestion, equivScore);
            if (suggestion.getId() != null) {
                scorerComponent.addComponentResult(
                        suggestion.getId(),
                        String.valueOf(equivScore.asDouble())
                );
            }
        }

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Optional<Score> getParentScore(Item subject, ResultDescription desc, Item suggestion) {
        if(!subject.getPublisher().equals(Publisher.BARB_TRANSMISSIONS)
                || contentResolver == null
                || suggestion.getContainer() == null
        ) {
            return Optional.empty();
        }

        Maybe<Identified> resolved = contentResolver.findByCanonicalUris(
                ImmutableSet.of(suggestion.getContainer().getUri())
        ).getFirstValue();
        if(resolved.hasValue()) {
            Content parent = (Content) resolved.requireValue();
            if(parent instanceof Series) { //this shouldn't be required but is being included as a sanity check
                Series series = (Series) parent;
                if(series.getParent() != null) {
                    resolved = contentResolver.findByCanonicalUris(
                            ImmutableSet.of(suggestion.getContainer().getUri()))
                    .getFirstValue();
                    if(resolved.hasValue()) {
                        parent = (Content) resolved.requireValue();
                    }
                }
            }
            Score parentScore = score(subject, parent, desc);
            return Optional.of(parentScore);
        }
        return Optional.empty();
    }

    private Score score(Item subject, Content suggestion, ResultDescription desc) {
        Score score = Score.nullScore();
        if (!Strings.isNullOrEmpty(subject.getTitle())) {
            if (Strings.isNullOrEmpty(suggestion.getTitle())) {
                desc.appendText("No Title (%s) scored: %s", suggestion.getCanonicalUri(), score);
            } else {
                score = score(subject, suggestion);
                desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);
            }
        }
        return score;
    }


    public Score score(Item subject, Content suggestion) {
        String subjectTitle = subject.getTitle().trim().toLowerCase();
        String suggestionTitle = suggestion.getTitle().trim().toLowerCase();

        //TxLog titles are size capped, so truncate everything if we are equiving to txlogs
        if (suggestion.getPublisher().equals(Publisher.BARB_TRANSMISSIONS)
            || (subject.getPublisher().equals(Publisher.BARB_TRANSMISSIONS))) {

            if (subjectTitle.length() > TXLOG_TITLE_LENGTH) {
                subjectTitle = subjectTitle.substring(0, TXLOG_TITLE_LENGTH);
            }
            if (suggestionTitle.length() > TXLOG_TITLE_LENGTH) {
                suggestionTitle = suggestionTitle.substring(0, TXLOG_TITLE_LENGTH);
            }
        }

        //if either subject or candidate is txlog, strip the year from the end
        if (suggestion.getPublisher().equals(Publisher.BARB_TRANSMISSIONS)) {
            suggestionTitle = removeArbitraryTrailingYear(suggestionTitle);
        }
        if (subject.getPublisher().equals(Publisher.BARB_TRANSMISSIONS)) {
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
        String remainingTitle = title;
        boolean removedAtLeastOne;
        do {
            removedAtLeastOne = false;
            for (String prefix : PREFIXES) {
                if (remainingTitle.length() > prefix.length() &&
                    remainingTitle.startsWith(prefix)) {
                    remainingTitle = remainingTitle.substring(prefix.length()).trim();
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

    @Override
    public String toString() {
        return "Barb Title-matching Item Scorer";
    }

    public static final class Builder {
        private Score scoreOnPerfectMatch = DEFAULT_SCORE_ON_PERFECT_MATCH;
        private Score scoreOnPartialMatch = DEFAULT_SCORE_ON_PARTIAL_MATCH;
        private Score scoreOnMismatch = DEFAULT_SCORE_ON_MISMATCH;
        private ContentResolver contentResolver;

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

        public BarbTitleMatchingItemScorer build() {
            return new BarbTitleMatchingItemScorer(this);
        }
    }
}
