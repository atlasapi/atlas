package org.atlasapi.remotesite.wikipedia.testutils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.ArticleFetcher;
import org.atlasapi.remotesite.wikipedia.film.FilmArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;

public class FilesystemArticlesSource implements FilmArticleTitleSource, ArticleFetcher {
    static Logger log = LoggerFactory.getLogger(FilesystemArticlesSource.class);
    Path directory;
    
    public FilesystemArticlesSource(Path directory) {
        this.directory = directory;
    }
    
    @Override
    public Iterable<String> getAllFilmArticleTitles() {
        try {
            final Iterator<Path> files = Files.newDirectoryStream(directory).iterator();
            return new Iterable<String>() {
                @Override
                public Iterator<String> iterator() {
                    return new AbstractIterator<String>() {
                        @Override
                        protected String computeNext() {
                            while(files.hasNext()) {
                                String name = files.next().getFileName().toString();
                                if(! name.endsWith(".mediawiki")) {
                                    continue;
                                }
                                return name.substring(0, name.length()-10);
                            }
                            return endOfData();
                        }
                    };
                }
            };
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Article fetchArticle(final String title) {
        try {
            final String source = com.google.common.io.Files.toString(directory.resolve(title + ".mediawiki").toFile(), Charsets.UTF_8);
            return new Article() {
                @Override
                public DateTime getLastModified() {
                    return new DateTime();
                }

                @Override
                public String getMediaWikiSource() {
                    return source;
                }

                @Override
                public String getTitle() {
                    return title;
                }
            };
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main(String... args) {
        FilesystemArticlesSource filesystemArticlesSource = new FilesystemArticlesSource(new File(System.getProperty("user.home") + "/atlasTestCaches/wikipedia/films").toPath());
        FilmExtractor ex = new FilmExtractor();
        for (String name : filesystemArticlesSource.getAllFilmArticleTitles())
        {
            System.out.println(name);
            try {
                ex.extract(filesystemArticlesSource.fetchArticle(name));
            } catch (Exception e) {
                log.error("Failed to extract \"" + name + "\"", e);
            }
        }
    }
    
}
