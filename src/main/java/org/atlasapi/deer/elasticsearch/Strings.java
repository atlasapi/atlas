package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class Strings {

    public static List<String> tokenize(String value, boolean filterStopWords) {
        List<String> tokensAsStrings = Lists.newArrayList();
        try {
            TokenStream tokens;
            if (filterStopWords) {
                tokens = new StandardAnalyzer(Version.LATEST)
                        .tokenStream("", new StringReader(value));
            } else {
                tokens = new StandardAnalyzer(Version.LATEST, CharArraySet.EMPTY_SET)
                        .tokenStream("", new StringReader(value));
            }
            tokens.reset();
            while (tokens.incrementToken()) {
                CharTermAttribute token = tokens.getAttribute(CharTermAttribute.class);
                tokensAsStrings.add(token.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (tokensAsStrings.isEmpty() && filterStopWords) {
            return tokenize(value, false);
        } else {
            return tokensAsStrings;
        }
    }

    public static String flatten(String value) {
        return Joiner.on("")
                .join(tokenize(value, true))
                .replaceAll("[^a-zA-Z0-9]", "")
                .toLowerCase();
    }
}
