package org.atlasapi.remotesite.wikipedia.people;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.sweble.wikitext.lazy.parser.InternalLink;
import org.sweble.wikitext.lazy.parser.Itemization;
import org.sweble.wikitext.lazy.parser.ItemizationItem;
import org.sweble.wikitext.lazy.parser.LazyParsedPage;
import org.sweble.wikitext.lazy.parser.Section;
import org.sweble.wikitext.lazy.parser.Table;
import org.sweble.wikitext.lazy.parser.TableCell;
import org.sweble.wikitext.lazy.parser.TableRow;

import com.google.common.collect.ImmutableList;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import xtc.parser.ParseException;

public class FootballListScrapper {

    public static Collection<String> extractNames(String indexText) throws IOException,
            ParseException {
        AstNode indexAST = SwebleHelper.parse(indexText);

        Visitor v = new Visitor();
        v.go(indexAST);
        return v.list;
    }

    protected static final class Visitor extends AstVisitor {
        LinkedList<String> list = new LinkedList<String>();

        public void visit(LazyParsedPage p) {
            iterate(p.getContent());
        }

        public void visit(Section s) {
            if ("See also".equalsIgnoreCase(SwebleHelper.flattenTextNodeList(s.getTitle()))) {
                return;  // skip the 'see also' section
            }
            iterate(s.getBody());
        }

        public void visit(Itemization i) {
            iterate(i.getContent());
        }

        public void visit(ItemizationItem i) {
            iterate(i.getContent());
        }

        public void visit(InternalLink l) {
            String target = l.getTarget();
            if (!target.startsWith("List of ")) {
                return;
            }
            list.add(l.getTarget());
        }

        @Override
        protected Object visitNotFound(AstNode node) { return null; }
    }

    public static Collection<String> extractOneList(String indexText) throws IOException,
            ParseException {
        AstNode indexAST = SwebleHelper.parse(indexText);

        ListVisitor v = new ListVisitor();
        v.go(indexAST);
        return v.list;
    }

    protected static final class ListVisitor extends AstVisitor {
        LinkedList<String> list = new LinkedList<String>();

        public void visit(LazyParsedPage p) {
            iterate(p.getContent());
        }

        public void visit(Section s) {
            if ("See also".equalsIgnoreCase(SwebleHelper.flattenTextNodeList(s.getTitle()))) {
                return;  // skip the 'see also' section
            }
            iterate(s.getBody());
        }

        public void visit(Table t) {
            iterate(t.getBody());
        }

        public void visit(TableRow t) {
            iterate(t.getBody());
        }

        public void visit(TableCell c) {
            iterate(c.getBody());
        }

        public void visit(Itemization i) {
            iterate(i.getContent());
        }

        public void visit(ItemizationItem i) {
            iterate(i.getContent());
        }

        public void visit(InternalLink l) {
            String target = l.getTarget().toLowerCase();
            if (target.contains("category")
                || target.contains("fifa")
                || target.contains("uefa")
                || target.contains("association")
                || target.contains("football")
                || target.contains("list")
                || target.contains("team")
                || target.contains("national")
                || target.contains("cup")
                || target.contains("champion")
                || target.contains("world")
                || target.contains("copa")
                || target.contains("game")) {
                return;
            }
            list.add(l.getTarget());
        }

        @Override
        protected Object visitNotFound(AstNode node) { return null; }
    }

}
