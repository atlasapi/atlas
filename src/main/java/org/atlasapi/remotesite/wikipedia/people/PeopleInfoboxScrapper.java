package org.atlasapi.remotesite.wikipedia.people;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;
import org.sweble.wikitext.lazy.preprocessor.Template;
import org.sweble.wikitext.lazy.preprocessor.TemplateArgument;
import xtc.parser.ParseException;

import java.io.IOException;
import java.util.Iterator;

public class PeopleInfoboxScrapper {
    private final static Logger log = LoggerFactory.getLogger(PeopleInfoboxScrapper.class);

    public static class Result {
        public String name;
        public String secondName;
        public String alias;
        public String image;
        public String website;
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
            String name = SwebleHelper.flattenTextNodeList(t.getName()).toLowerCase();
            boolean isFootballInfobox = name.contains("infobox") &&  name.contains("person");
            if (isFootballInfobox) {
                Iterator<AstNode> children = t.getArgs().iterator();
                while(children.hasNext()) {
                    consumeAttribute(children.next());
                }
            } else if ("IMDb name".equalsIgnoreCase(name)) {
                try {
                    String imdbID = SwebleHelper.extractArgument(t, 0);
                    attrs.alias = "http://imdb.com/name/nm" + imdbID;
                } catch (Exception e) {
                    log.warn("Failed to extract IMDB ID from \""+ SwebleHelper.unparse(t) +"\"", e);
                }
            }
        }

        void consumeAttribute(AstNode n) throws IOException, ParseException {
            if (!(n instanceof TemplateArgument)) {
                return;
            }
            TemplateArgument a = (TemplateArgument) n;
            final String key = SwebleHelper.flattenTextNodeList(a.getName());
            //TODO implement consuming
        }
    }
}
