package org.atlasapi.remotesite.wikipedia.film;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper.ListItemResult;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;
import org.sweble.wikitext.lazy.preprocessor.Template;
import org.sweble.wikitext.lazy.preprocessor.TemplateArgument;

import xtc.parser.ParseException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import de.fau.cs.osr.ptk.common.ast.NodeList;

/**
 * This utility class extracts information from the Film infobox in a Wikipedia article.
 */
public final class FilmInfoboxScraper {
    private final static Logger log = LoggerFactory.getLogger(FilmInfoboxScraper.class);
    
    public static class ReleaseDateResult {
        public ReleaseDateResult(String year, String month, String day) {
            this.year = year; this.month = month; this.day = day;
        }
        public ReleaseDateResult(){}

        private String year;
        private String month;
        private String day;
        private ListItemResult location;
        
        public String year() {
            return year;
        }
        public String month() {
            return month;
        }
        public String day() {
            return day;
        }
        public ListItemResult location() {
            return location;
        }

        private static enum Field {YEAR, MONTH, DAY, LOCATION};
    }
    
    public static class Result {
        public ImmutableList<ListItemResult> name;
        public ImmutableList<ListItemResult> directors;
        public ImmutableList<ListItemResult> producers;
        public ImmutableList<ListItemResult> writers;
        public ImmutableList<ListItemResult> screenplayWriters;
        public ImmutableList<ListItemResult> storyWriters;
        public ImmutableList<ListItemResult> narrators;
        public ImmutableList<ListItemResult> starring;
        public ImmutableList<ListItemResult> composers;
        public ImmutableList<ListItemResult> cinematographers;
        public ImmutableList<ListItemResult> editors;
        public ImmutableList<ListItemResult> productionStudios;
        public ImmutableList<ListItemResult> distributors;
        public ImmutableList<ReleaseDateResult> releaseDates;
        public Integer runtimeInMins;
        public ImmutableList<ListItemResult> countries;
        public ImmutableList<ListItemResult> language;
        public Map<String,String> externalAliases = new TreeMap<>();
    }

    /**
     * Returns the information given to the Film infobox template in the given Mediawiki page source.
     */
    public static Result getInfoboxAttrs(String articleText) throws IOException, ParseException {
        LazyPreprocessedPage ast = SwebleHelper.preprocess(articleText, false);

        InfoboxVisitor v = new InfoboxVisitor();
        Iterator<AstNode> topLevelEls = ast.getContent().iterator();
        while(topLevelEls.hasNext()) {
            v.consumeInfobox(topLevelEls.next());
        }

        return v.attrs;
    }

    /**
     * This thing looks at a preprocessor-generated AST of a Mediawiki page, finds the Film infobox, and gathers key-value pairs from it.
     */
    private static final class InfoboxVisitor {
        final Result attrs = new Result();

        void consumeInfobox(AstNode n) throws IOException, ParseException {
            if (!(n instanceof Template)) {
                return;
            }
            Template t = (Template) n;

            String name = SwebleHelper.flattenTextNodeList(t.getName());
            if ("Infobox film".equalsIgnoreCase(name)) {
                Iterator<AstNode> children = t.getArgs().iterator();
                while(children.hasNext()) {
                    consumeAttribute(children.next());
                }
            } else if ("IMDb title".equalsIgnoreCase(name)) {
                try {
                    String imdbID = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("imdb:title", imdbID);
                    attrs.externalAliases.put("imdb:url", "http://imdb.com/title/tt" + imdbID);
                } catch (Exception e) {
                    log.warn("Failed to extract IMDB ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            } else if ("AllRovi movie".equalsIgnoreCase(name)) {
                try {
                    String id = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("allrovi:movie", id);
                } catch (Exception e) {
                    log.warn("Failed to extract AllMovie ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            } else if ("rotten-tomatoes".equalsIgnoreCase(name)) {
                try {
                    String id = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("rottentomatoes:movie", id);
                } catch (Exception e) {
                    log.warn("Failed to extract Rotten Tomatoes ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            } else if ("Mojo title".equalsIgnoreCase(name)) {
                try {
                    String id = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("boxofficemojo:movie", id);
                } catch (Exception e) {
                    log.warn("Failed to extract Box Office Mojo ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            } else if ("Metacritic film".equalsIgnoreCase(name)) {
                try {
                    String id = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("metacritic:movie", id);
                } catch (Exception e) {
                    log.warn("Failed to extract Metacritic ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            }
        }

        void consumeAttribute(AstNode n) throws IOException, ParseException {
            if (!(n instanceof TemplateArgument)) {
                return;
            }
            TemplateArgument a = (TemplateArgument) n;
            final String key = SwebleHelper.flattenTextNodeList(a.getName());
            
            if ("name".equalsIgnoreCase(key)) {
                attrs.name = SwebleHelper.extractList(a.getValue());
            } else if ("director".equalsIgnoreCase(key)) {
                attrs.directors = SwebleHelper.extractList(a.getValue());
            } else if ("producer".equalsIgnoreCase(key)) {
                attrs.producers = SwebleHelper.extractList(a.getValue());
            } else if ("writer".equalsIgnoreCase(key)) {
                attrs.writers = SwebleHelper.extractList(a.getValue());
            } else if ("screenplay".equalsIgnoreCase(key)) {
                attrs.screenplayWriters = SwebleHelper.extractList(a.getValue());
            } else if ("story".equalsIgnoreCase(key)) {
                attrs.storyWriters = SwebleHelper.extractList(a.getValue());
            } else if ("narrator".equalsIgnoreCase(key)) {
                attrs.narrators = SwebleHelper.extractList(a.getValue());
            } else if ("starring".equalsIgnoreCase(key)) {
                attrs.starring = SwebleHelper.extractList(a.getValue());
            } else if ("music".equalsIgnoreCase(key)) {
                attrs.composers = SwebleHelper.extractList(a.getValue());
            } else if ("cinematography".equalsIgnoreCase(key)) {
                attrs.cinematographers = SwebleHelper.extractList(a.getValue());
            } else if ("editing".equalsIgnoreCase(key)) {
                attrs.editors = SwebleHelper.extractList(a.getValue());
            } else if ("studio".equalsIgnoreCase(key)) {
                attrs.productionStudios = SwebleHelper.extractList(a.getValue());
            } else if ("distributor".equalsIgnoreCase(key)) {
                attrs.distributors = SwebleHelper.extractList(a.getValue());
            } else if ("released".equalsIgnoreCase(key)) {
                attrs.releaseDates = extractFilmReleaseDates(a.getValue());
            } else if ("runtime".equalsIgnoreCase(key)) {
                String runtimeText = SwebleHelper.normalizeAndFlattenTextNodeList(a.getValue());
                try {
                    attrs.runtimeInMins = Integer.parseInt(runtimeText.split(" ", 2)[0]);
                    if (attrs.runtimeInMins < 10 || attrs.runtimeInMins > 300) {
                        log.warn("Suspicious running time: " + attrs.runtimeInMins);
                    }
                } catch (NumberFormatException e) {
                    if (! runtimeText.isEmpty()) {
                        log.warn("Failed to extract running time from weird string \"" + runtimeText + "\"");
                    }
                }
            } else if ("country".equalsIgnoreCase(key)) {
                attrs.countries = SwebleHelper.extractList(a.getValue());
            } else if ("language".equalsIgnoreCase(key)) {
                attrs.language = SwebleHelper.extractList(a.getValue());
            }
        }
    }
    
    private static final Pattern britishDatePattern = Pattern.compile("(\\d{1,2}) ([A-Z]?[a-z]+),? (\\d{4})");
    private static final Pattern americanDatePattern = Pattern.compile("([A-Z]?[a-z]+) (\\d{1,2}),? (\\d{4})");
    
    private static ImmutableList<ReleaseDateResult> extractFilmReleaseDates(NodeList a) {
        ImmutableList.Builder<ReleaseDateResult> builder = ImmutableList.builder();
        (new FilmDateVisitor(builder)).go(a);
        ImmutableList<ReleaseDateResult> templateResults = builder.build();
        if (! templateResults.isEmpty()) {
            return templateResults;
        }
        
        // Otherwise, we fall back on the plain text method...
        //
        String maybeDateText = SwebleHelper.normalizeAndFlattenTextNodeList(a);
        String y, m, d;
        Matcher americanMatcher = americanDatePattern.matcher(maybeDateText);
        if (americanMatcher.find()) {
            m = americanMatcher.group(1);
            d = americanMatcher.group(2);
            y = americanMatcher.group(3);
        } else {
            Matcher britishMatcher = britishDatePattern.matcher(maybeDateText);
            if (britishMatcher.find()) {
                d = britishMatcher.group(1);
                m = britishMatcher.group(2);
                y = britishMatcher.group(3);
            } else {
                return ImmutableList.of();
            }
        }
        try {
            m = Integer.toString(LocalDate.parse(m, DateTimeFormat.forPattern("MMM").withLocale(Locale.ENGLISH)).getMonthOfYear());
            return ImmutableList.of(new ReleaseDateResult(y,m,d));
        } catch (Exception e) {
            log.warn("Couldn't interpret month \"" + m + "\" – skipping release date \"" + maybeDateText + "\"");
            return ImmutableList.of();
        }
    }
    
    protected static class FilmDateVisitor extends AstVisitor {
        private final ImmutableList.Builder<ReleaseDateResult> builder;
        // State:
        private ReleaseDateResult.Field nextField = ReleaseDateResult.Field.YEAR;
        private ReleaseDateResult currentResult;
        
        public FilmDateVisitor(Builder<ReleaseDateResult> builder) {
            super();
            this.builder = builder;
        }

        public void visit(NodeList l) {
            iterate(l);
        }
        
        public void visit(Template t) {
            String name = SwebleHelper.flattenTextNodeList(t.getName());
            if (! "Film date".equalsIgnoreCase(name)) {
                return;
            }
            for (AstNode n : t.getArgs()) {
                if (!(n instanceof TemplateArgument)) {
                    log.warn("Encountered a non-TemplateArgument; ignoring");
                    continue;
                }
                TemplateArgument a = (TemplateArgument) n;
                if (a.getHasName()) {
                    continue;  // skip named arguments as they're only going to be 'ref=' which we don't care about
                }
                
                if (nextField == ReleaseDateResult.Field.YEAR) {
                    currentResult = new ReleaseDateResult();
                    builder.add(currentResult);
                    currentResult.year = SwebleHelper.flattenTextNodeList(a.getValue());
                    nextField = ReleaseDateResult.Field.MONTH;
                } else if (nextField == ReleaseDateResult.Field.MONTH) {
                    currentResult.month = SwebleHelper.flattenTextNodeList(a.getValue());
                    nextField = ReleaseDateResult.Field.DAY;
                } else if (nextField == ReleaseDateResult.Field.DAY) {
                    currentResult.day = SwebleHelper.flattenTextNodeList(a.getValue());
                    nextField = ReleaseDateResult.Field.LOCATION;
                } else if (nextField == ReleaseDateResult.Field.LOCATION) {
                    try {
                        ImmutableList<ListItemResult> list = SwebleHelper.extractList(a.getValue());
                        if (! list.isEmpty()) {
                            currentResult.location = list.get(0);
                        }
                    } catch (Exception e) {
                        log.warn("Extracting release date location failed: " + SwebleHelper.unparse(a.getValue()), e);
                    }
                    nextField = ReleaseDateResult.Field.YEAR;
                }
            }
        }
        
        @Override
        protected Object visitNotFound(AstNode node) { return null; }
    }

}
