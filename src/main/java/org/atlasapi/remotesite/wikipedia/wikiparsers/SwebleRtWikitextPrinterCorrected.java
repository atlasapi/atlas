package org.atlasapi.remotesite.wikipedia.wikiparsers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import org.sweble.wikitext.lazy.AstNodeTypes;
import org.sweble.wikitext.lazy.parser.RtData;
import org.sweble.wikitext.lazy.utils.RtWikitextPrinter;

import de.fau.cs.osr.ptk.common.ast.AstNode;
import de.fau.cs.osr.ptk.common.ast.NodeList;
import de.fau.cs.osr.ptk.common.ast.StringContentNode;

/**
 * A complete copy of {@link org.sweble.wikitext.lazy.utils.RtWikitextPrinter}, slightly modified to fix a problem when reconstructing parsed pages in order to then preprocess them.
 * <p>
 * Parts of nested templates are interpreted as 'XML attribute garbage' which wasn't included by {@link RtWikitextPrinter} in the reconstructed source. This class <em>does</em> include it.
 * It was necessary to duplicate the whole class as the fix is to a private method.
 */
public class SwebleRtWikitextPrinterCorrected
{
    private PrintWriter w;
    
    // =========================================================================
    
    public SwebleRtWikitextPrinterCorrected(Writer writer)
    {
        this.w = new PrintWriter(writer);
    }
    
    // =========================================================================
    
    public static String print(AstNode node)
    {
        StringWriter writer = new StringWriter();
        new SwebleRtWikitextPrinterCorrected(writer).go(node);
        return writer.toString();
    }
    
    public static Writer print(Writer writer, AstNode node)
    {
        new SwebleRtWikitextPrinterCorrected(writer).go(node);
        return writer;
    }
    
    // =========================================================================
    
    private void go(AstNode node)
    {
        switch (node.getNodeType())
        {
            case AstNode.NT_NODE_LIST:
            {
                for (AstNode c : (NodeList) node)
                    go(c);
                
                break;
            }
                
            case AstNodeTypes.NT_XML_ATTRIBUTE_GARBAGE:  // <-- this line is the only change I've made
            case AstNode.NT_TEXT:
            {
                w.print(((StringContentNode) node).getContent());
                break;
            }
                
            default:
            {
                RtData rtd = (RtData) node.getAttribute("RTD");
                if (rtd != null)
                {
                    int i = 0;
                    for (AstNode n : node)
                    {
                        printRtd(rtd.getRts()[i++]);
                        if (n != null)
                            go(n);
                    }
                    printRtd(rtd.getRts()[i]);
                }
                else
                {
                    for (AstNode n : node)
                    {
                        if (n != null)
                            go(n);
                    }
                }
                break;
            }
        }
    }
    
    private void printRtd(Object[] objects)
    {
        if (objects != null)
        {
            for (Object o : objects)
            {
                if (o instanceof AstNode)
                {
                    go((AstNode) o);
                }
                else
                {
                    w.print(o);
                }
            }
        }
    }
}
