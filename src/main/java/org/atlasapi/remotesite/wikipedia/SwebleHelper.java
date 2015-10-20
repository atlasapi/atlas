package org.atlasapi.remotesite.wikipedia;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.LazyParser;
import org.sweble.wikitext.lazy.LazyPreprocessor;
import org.sweble.wikitext.lazy.ParserConfigInterface;
import org.sweble.wikitext.lazy.parser.InternalLink;
import org.sweble.wikitext.lazy.parser.Itemization;
import org.sweble.wikitext.lazy.parser.ItemizationItem;
import org.sweble.wikitext.lazy.parser.LazyParsedPage;
import org.sweble.wikitext.lazy.parser.SemiPre;
import org.sweble.wikitext.lazy.parser.SemiPreLine;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;
import org.sweble.wikitext.lazy.preprocessor.Template;
import org.sweble.wikitext.lazy.preprocessor.TemplateArgument;
import org.sweble.wikitext.lazy.utils.SimpleParserConfig;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import xtc.parser.ParseException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import de.fau.cs.osr.ptk.common.ast.NodeList;
import de.fau.cs.osr.ptk.common.ast.Text;

/**
 * Contains helper methods for dealing with the SWEBLE wikitext parser ({@link org.sweble.wikitext.lazy}) and its AST output.
 */
public class SwebleHelper {
    private static final Logger log = LoggerFactory.getLogger(SwebleHelper.class);
    private static final ParserConfigInterface cfg = new SimpleParserConfig();
    private static final LazyPreprocessor preprocessor = new LazyPreprocessor(cfg);

    /**
     * Performs the first half of the Mediawiki parsing process -- the resulting AST includes templates and their arguments (the usual intention being to expand and include these before the remaining parse) but no awareness of formatting or any other textual abnormalities.
     * @param includeOnly If true, the page is processed as if it's being included – everything outside of the 'includeonly' section is ignored. This is handy for instance in the case of TV season pages, which when treated as a template in this way, include only the episodes table.
     * @see #parse(java.lang.String)
     * @see <a href="http://sweble.org/downloads/diwp-preprint.pdf">SWEBLE spec pdf</a>
     */
    public static LazyPreprocessedPage preprocess(String mediaWikiSource, boolean includeOnly) {
        try {
            return (LazyPreprocessedPage) preprocessor.parseArticle(mediaWikiSource, "", includeOnly);
        } catch (IOException|ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Performs the second half of the Mediawiki parsing process -- the resulting AST includes links, formatting, sections etc., but any template inclusions remain in their useless plain text form.
     * @see #preprocess(java.lang.String, boolean)
     * @see <a href="http://sweble.org/downloads/diwp-preprint.pdf">SWEBLE spec pdf</a>
     */
    public static LazyParsedPage parse(String mediaWikiSource) {
        try {
            return (LazyParsedPage) new LazyParser(cfg).parseArticle(mediaWikiSource, "");
        } catch (IOException|ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Prints a representation of the given AST to System.out
     * @param indent How many spaces to indent by at the start of each line.
     */
    public static void showAST(AstNode ast, int indent) {
        for (int i=0; i<indent; i++) {
            System.out.print(ast.getNodeName().equals("Template") ? "* " : "  ");
        }
        System.out.println("* " + ast.getNodeName() + " [" + ast.getNodeTypeName() + "]");
        for (Map.Entry attr : ast.getAttributes().entrySet()) {
            for (int i=0; i<indent; i++) {
                System.out.print("  ");
            }
            System.out.println("   " + attr.getKey() + " : " + attr.getValue());
        }
        if (ast instanceof Text) {
            for (int i=0; i<indent; i++) {
                System.out.print("  ");
            }
            System.out.println("  => " + ((Text) ast).getContent());
        }
        Iterator<AstNode> children = ast.iterator();
        while (children.hasNext()) {
            showAST(children.next(), indent + 1);
        }
    }


    /**
     * Recovers the original source of the given AST portion.
     */
    public static String unparse(AstNode n) {
        return SwebleRtWikitextPrinterCorrected.print(n);
    }
    
    /**
     * Quick and dirty way to flatten a NodeList containing a bunch of Text nodes into a single string -- for instance, to process them with the other half of the parsing process (or some other form of interpretation) and get information out.
     * 
     * Watch out though, because any other, non-Text nodes will be ignored...
     */
    public static String flattenTextNodeList(NodeList l) {
        StringBuilder b = new StringBuilder(600);
        Iterator<AstNode> children = l.iterator();
        AstNode n;
        while (children.hasNext()) {
            n = children.next();
            if (n instanceof Text) {
                b.append(((Text) n).getContent());
            }
        }
        return b.toString().trim();
    }
    
    /**
     * Returns a positional template argument, passed through {@link #flattenTextNodeList(NodeList)}.
     */
    public static String extractArgument(Template t, int pos) throws IndexOutOfBoundsException {
        NodeList args = t.getArgs();
        return flattenTextNodeList(((TemplateArgument) args.get(pos)).getValue());
    }
    
    /**
     * Tries to interpret the given AST as a {@link LocalDate}. The AST should be of the preprocessed (not parsed) type, and should contain no other dubious content.
     */
    public static Optional<LocalDate> extractDate(AstNode node) {
        try {
            Object result = new DateVisitor().go(node);
            if (result != null && result instanceof LocalDate) {
                return Optional.of((LocalDate) result);
            }
        } catch (Exception e) {
            log.warn("Failed to extract date from node \""+ unparse(node) +"\"");
        }
        
        return Optional.absent();
    }
    
    protected static class DateVisitor extends AstVisitor {
        public LocalDate visit(NodeList l) {
            for (AstNode n : l) {
                Object result = go(n);
                if (result != null && result instanceof LocalDate) {
                    return (LocalDate) result;
                }
            }
            return null;
        }
        public LocalDate visit(Template t) {
            String name = SwebleHelper.flattenTextNodeList(t.getName());
            if (! "Start date".equalsIgnoreCase(name) && ! "End date".equalsIgnoreCase(name)) {
                return null;
            }
            try {
                NodeList args = t.getArgs();
                String y = flattenTextNodeList(((TemplateArgument) args.get(0)).getValue());
                String m = flattenTextNodeList(((TemplateArgument) args.get(1)).getValue());
                String d = flattenTextNodeList(((TemplateArgument) args.get(2)).getValue());
                return new LocalDate(Integer.parseInt(y), Integer.parseInt(m), Integer.parseInt(d));
            } catch (Exception e) {
                log.warn("Failed to extract date from date template \""+ unparse(t) +"\"");
            }
            return null;
        }
        @Override
        protected Object visitNotFound(AstNode node) {
            return null;
        }
    }
    
    public static class ListItemResult {
        public final String name;
        public final Optional<String> articleTitle;
        public ListItemResult(String name, Optional<String> articleTitle) {
            this.name = normalize(checkNotNull(name));
            this.articleTitle = articleTitle;
        }
        protected String normalize(String text) {
            WikiModel model = new WikiModel("","");
            String noMarkUp = model.render( new PlainTextConverter(),text);
            return noMarkUp;
        }

        @Override
        public String toString() {
            return name + (articleTitle.isPresent() ? " (=> " + articleTitle.get() + ")" : "");
        }
    }
    
    /**
     * Attempts to extract a 'list of string/link values'-style template argument in a highly generic way.
     * @param node A preprocessed (not parsed) AST of said argument.
     */
    public static ImmutableList<ListItemResult> extractList(AstNode node) {
        ImmutableList.Builder<ListItemResult> builder = ImmutableList.builder();
        String unparsed = unparse(node);
        unparsed = plainlistTemplatePattern.matcher(unparsed).replaceAll("");  // Basically since {{plainlist}} is just a wrapper for a normal itemization, we can throw it away.
        unparsed = stupidBracesPattern.matcher(unparsed).replaceAll("");       // See: http://en.wikipedia.org/wiki/Template:Plainlist
        new ListVisitor(builder).go(parse(unparsed));
        return builder.build();
    }
    
    private static final Pattern plainlistTemplatePattern = Pattern.compile("\\s*\\{\\{\\s*(end)?plainlist\\s*(\\}\\}|\\|)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern stupidBracesPattern = Pattern.compile("\\s*\\}\\}\\s*");
    
    protected static class ListVisitor extends AstVisitor {
        private final ImmutableList.Builder<ListItemResult> builder;
        public ListVisitor(ImmutableList.Builder<ListItemResult> builder) {
            this.builder = checkNotNull(builder);
        }
        
        // State:
        public String linkTargetTitle = null;
        
        public void visit(LazyParsedPage value) {
            iterate(value.getContent());
        }
        public void visit(SemiPre wtf) {
            iterate(wtf.getContent());
        }
        public void visit(SemiPreLine wtf) {
            iterate(wtf.getContent());
        }
        public void visit(Itemization i) {
            iterate(i.getContent());
        }
        public void visit(ItemizationItem i) {
            iterate(i.getContent());
        }
        public void visit(InternalLink link) {
            NodeList titleContent = link.getTitle().getContent();
            if(titleContent.isEmpty()) {
                builder.add(new ListItemResult(link.getTarget(), Optional.of(link.getTarget())));
            } else {
                linkTargetTitle = link.getTarget();
                iterate(titleContent);
                linkTargetTitle = null;
            }
        }
        public void visit(Text t) {
            String name = t.getContent().trim();
            if (name.isEmpty() || name.startsWith("(")) {  // Things that start with brackets are probably extraneous annotations, not names -- skip them.
                return;
            }
            builder.add(new ListItemResult(name, Optional.fromNullable(linkTargetTitle)));
        }

        @Override
        protected Object visitNotFound(AstNode node) { return null; }

    }

}
