package org.atlasapi.equiv.scorers;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

public class BarbTitleMatchingItemScorer implements EquivalenceScorer<Item> {

    public static final String NAME = "Barb-Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of(
            "the ", "Live ", "FILM: ", "FILM:", "NEW: ", "NEW:", "LIVE: ", "LIVE:"
    );
    private static final ImmutableSet<String> POSTFIXES = ImmutableSet.of("\\(Unrated\\)", "\\(Rated\\)");
    private static final Pattern TRAILING_APOSTROPHE_PATTERN = Pattern.compile("\\w' ");
    private static final Score DEFAULT_SCORE_ON_PERFECT_MATCH = Score.valueOf(2D);
    private static final Score DEFAULT_SCORE_ON_MISMATCH = Score.nullScore();
    private static final int TXLOG_TITLE_LENGTH = 40;
    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();

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

    private final Score scoreOnPerfectMatch;
    private final Score scoreOnMismatch;

    public BarbTitleMatchingItemScorer() {
        this(DEFAULT_SCORE_ON_PERFECT_MATCH, DEFAULT_SCORE_ON_MISMATCH);
    }

    public BarbTitleMatchingItemScorer(Score scoreOnPerfectMatch, Score scoreOnMismatch) {
        this.scoreOnPerfectMatch = checkNotNull(scoreOnPerfectMatch);
        this.scoreOnMismatch = checkNotNull(scoreOnMismatch);
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

        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

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

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(Item subject, Item suggestion, ResultDescription desc) {
        Score score = Score.nullScore();
        if (!Strings.isNullOrEmpty(suggestion.getTitle())) {
            if (Strings.isNullOrEmpty(suggestion.getTitle())) {
                desc.appendText("No Title (%s) scored: %s", suggestion.getCanonicalUri(), score);
            } else {
                score = score(subject, suggestion);
                desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);
            }
        }
        return score;
    }


    private Score score(Item subject, Item suggestion) {
        String subjectTitle = subject.getTitle();
        if (suggestion.getPublisher().equals(Publisher.BARB_TRANSMISSIONS) && subjectTitle.length() > TXLOG_TITLE_LENGTH) {
            subjectTitle = subjectTitle.substring(0, TXLOG_TITLE_LENGTH);
        }

        if (subjectTitle.equals(suggestion.getTitle())) {
            return scoreOnPerfectMatch;
        }

        TitleType subjectType = TitleType.titleTypeOf(subject.getTitle());
        TitleType suggestionType = TitleType.titleTypeOf(suggestion.getTitle());


        Score score = Score.nullScore();

        if (subjectType == suggestionType) {
            subjectTitle = removePostfix(subjectTitle, subject.getYear());
            String suggestionTitle = removePostfix(suggestion.getTitle(), suggestion.getYear());
            return compareTitles(subjectTitle, suggestionTitle);

        }

        return score;
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
            return subjTitle.equals(suggTitle) ? Score.ONE : scoreOnMismatch;
        } else if (subjTitle.contains(":") && subjTitle.length() > suggTitle.length()) {

            String subjSubstring = subjTitle.substring(0, subjTitle.indexOf(":"));
            return subjSubstring.equals(suggTitle) ? Score.ONE : scoreOnMismatch;
        } else if (suggTitle.contains(":")) {

            String suggSubstring = suggTitle.substring(0, suggestionTitle.indexOf(":"));
            return suggSubstring.equals(subjTitle) ? Score.ONE : scoreOnMismatch;
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
        String withoutSequencePrefix = removeSequencePrefix(title);
        String expandedTitle = titleExpander.expand(withoutSequencePrefix);
        String withoutCommonPrefixes = removeCommonPrefixes(expandedTitle);
        return StringUtils.stripAccents(withoutCommonPrefixes);
    }

    private String normalizeRegularExpression(String title) {
        return regularExpressionReplaceSpecialChars(removeCommonPrefixes(removeSequencePrefix(title).toLowerCase()));
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
        String titleWithoutPrefix = title;
        for (String prefix : PREFIXES) {
            if (titleWithoutPrefix.length() > prefix.length() &&
                    titleWithoutPrefix.substring(0, prefix.length()).equalsIgnoreCase(prefix)) {
                titleWithoutPrefix = titleWithoutPrefix.substring(prefix.length());
            }
        }
        return titleWithoutPrefix;
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
}
