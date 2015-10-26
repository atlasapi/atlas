package org.atlasapi.remotesite.wikipedia.football;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import org.atlasapi.remotesite.wikipedia.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;
import org.sweble.wikitext.lazy.preprocessor.Template;
import org.sweble.wikitext.lazy.preprocessor.TemplateArgument;
import xtc.parser.ParseException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class TeamInfoboxScrapper {
    private final static Logger log = LoggerFactory.getLogger(TeamInfoboxScrapper.class);

    public static class Result {
        public ImmutableList<SwebleHelper.ListItemResult> clubname;
        public ImmutableSet<SwebleHelper.ListItemResult> nicknames;
        public Map<String,String> externalAliases = new TreeMap<>();
    }

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
     * This thing looks at a preprocessor-generated AST of a Mediawiki page, finds the Football infobox, and gathers key-value pairs from it.
     */
    private static final class InfoboxVisitor {
        final Result attrs = new Result();

        void consumeInfobox(AstNode n) throws IOException, ParseException {
            if (!(n instanceof Template)) {
                return;
            }
            Template t = (Template) n;

            String name = SwebleHelper.flattenTextNodeList(t.getName());
            if ("Infobox football club".equalsIgnoreCase(name)) {
                Iterator<AstNode> children = t.getArgs().iterator();
                while(children.hasNext()) {
                    consumeAttribute(children.next());
                }
            } else if ("Official website".equalsIgnoreCase(name)) {
                try {
                    String website = SwebleHelper.extractArgument(t, 0);
                    attrs.externalAliases.put("off:url", website);
                } catch (Exception e) {
                    log.warn("Failed to extract official website from \"" + SwebleHelper.unparse(t) + "\"", e);
                }
            }
        }

        void consumeAttribute(AstNode n) throws IOException, ParseException {
            if (!(n instanceof TemplateArgument)) {
                return;
            }
            TemplateArgument a = (TemplateArgument) n;
            final String key = SwebleHelper.normalizeAndFlattenTextNodeList(a.getName());

            if ("clubname".equalsIgnoreCase(key)) {
                attrs.clubname = SwebleHelper.extractList(a.getValue());
            } else if ("nickname".equalsIgnoreCase(key)) {
                attrs.nicknames.builder().addAll(SwebleHelper.extractList(a.getValue())).build();
            }
        }
    }
}
