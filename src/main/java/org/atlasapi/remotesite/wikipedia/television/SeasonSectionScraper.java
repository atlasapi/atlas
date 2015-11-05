package org.atlasapi.remotesite.wikipedia.television;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.wikipedia.wikiparsers.Callback;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.parser.LazyParsedPage;
import org.sweble.wikitext.lazy.parser.Section;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;

/**
 * Pulls out sections of an episode list article which correspond to seasons.
 */
public final class SeasonSectionScraper extends AstVisitor {
    private static final Logger log = LoggerFactory.getLogger(SeasonSectionScraper.class);
    
    public static class Result {
        public String name;
        public String content;
    }
    
    private final Callback<Result> resultCallback;
    /**
     * @param resultCallback Will be called for each found section.
     */
    public SeasonSectionScraper(Callback<Result> resultCallback) {
        this.resultCallback = resultCallback;
    }
    
    /** Courtesy exception in case of the wrong AST */
    public void visit(LazyPreprocessedPage p) {
        throw new IllegalArgumentException("The SeasonSectionScraper only knows how to deal with sections, so it wants a parsed page, not a preprocessed one.");
    }
    
    public void visit(LazyParsedPage p) {
        iterate(p.getContent());
    }
    
    private final Pattern seasonHeading = Pattern.compile("\\s*((season|series) (\\d+)).*", Pattern.CASE_INSENSITIVE);
    
    public void visit(final Section s) {
        String name = SwebleHelper.flattenTextNodeList(s.getTitle());
        final Matcher matcher = seasonHeading.matcher(name);
        if (matcher.matches()) {
            resultCallback.have(new Result(){{
                name = matcher.group(1);
                content = SwebleHelper.unparse(s.getBody());
            }});
            return;
        }
        iterate(s.getBody());
    }
    
    @Override
    protected Object visitNotFound(AstNode node) {
        log.debug("Skipping node " + node.getNodeName());
        return null;
    }
    
}
