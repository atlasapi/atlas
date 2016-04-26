package org.atlasapi.query.content;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.itv.ItvMercuryBrandAdapter;
import org.atlasapi.remotesite.youtube.YoutubeUriCanonicaliser;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.metabroadcast.common.base.Maybe;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class PerPublisherCurieExpander implements CurieExpander {

	public enum CurieAlgorithm {
		C4 {
		    
		    final Pattern c4ProgrammeIdCuriePattern = Pattern.compile("(\\d+)-(\\d+)");
			final String separator = "_";
			@Override
			public String expand(String curie) {
				Maybe<String> uri = expandC4Curie(curie);
				if (uri.hasValue()) {
					return uri.requireValue();
				}
				String withoutPrefix = curie.substring(curie.indexOf(":") + 1);
				Matcher matcher = c4ProgrammeIdCuriePattern.matcher(withoutPrefix);
				if(matcher.matches()) {
				    return String.format("http://www.channel4.com/programmes/%s/%s", matcher.group(1), matcher.group(2));
				}
				if (withoutPrefix.contains(separator)) {
					String[] components = withoutPrefix.split(separator);
					return String.format("http://www.channel4.com/programmes/%s/4od#%s", components[0], components[1]);
				} else {
				    Matcher m = c4CuriePattern.matcher(withoutPrefix);
				    if(m.matches()) {
				        StringBuilder expanded = new StringBuilder("http://www.channel4.com/programmes/").append(m.group(1));
				        if(m.group(2) != null) {
				            expanded.append("/episode-guide/");
				            expanded.append(m.group(2).substring(1));
				            if(m.group(3) != null) {
	                            expanded.append("/");
	                            expanded.append(m.group(3).substring(1));
				            }
				        }
				        return expanded.toString();
				    }
					return null;
				}
			}

			final Pattern c4programmeIdPattern = Pattern.compile("http://www.channel4.com/programmes/(\\d+)/(\\d+)");
			final Pattern c4CuriePattern = Pattern.compile("([a-zA-Z0-9-/]+?)(-series-\\d+)?(-episode-\\d+)?");

			final Pattern c4BrandIdPattern = Pattern.compile("http://www.channel4.com/programmes/([^\\./&=]+)(?:/episode-guide/(series-\\d+)(?:/(episode-\\d+))?)?");
			final Pattern c4odPattern = Pattern.compile("http://www.channel4.com/programmes/([^\\./&=]+)/4od#([^\\./&=]+).*");

			@Override
			public String compact(String url) {
				
				Maybe<String> curie = compactC4Uri(url);
				if (curie.hasValue()) {
					return curie.requireValue();
				}
				
				Matcher programmeIdMatcher = c4programmeIdPattern.matcher(url);
				if(programmeIdMatcher.matches()) {
				    return String.format("%s:%s-%s", this.name().toLowerCase(), programmeIdMatcher.group(1), programmeIdMatcher.group(2));
				}
				
				Matcher itemMatcher = c4odPattern.matcher(url);
				if (itemMatcher.matches()) {
					return String.format("%s:%s_%s", this.name().toLowerCase(), itemMatcher.group(1), itemMatcher.group(2));
				}

				Matcher brandIdMatcher = c4BrandIdPattern.matcher(url);
				if (brandIdMatcher.matches()) {
					String brandIdPossiblyWithSeries = brandIdMatcher.group(1);
					if (brandIdMatcher.group(2) != null) {
						brandIdPossiblyWithSeries += "-" + brandIdMatcher.group(2);
					}
					if (brandIdMatcher.group(3) != null) {
						brandIdPossiblyWithSeries += "-" + brandIdMatcher.group(3);
					}
					return String.format("%s:%s", this.name().toLowerCase(), brandIdPossiblyWithSeries);
				}
				
				return null;
			}
		},
		ITV {
			private static final String ITV_CURIE = "itv:";
			
			@Override
			public String expand(String curie) {
				if (curie.startsWith(ITV_CURIE)) {
				    return ItvMercuryBrandAdapter.BASE_URL+curie.replace(ITV_CURIE, "");
				}
				return null;
			}

			@Override
			public String compact(String url) {
				Matcher matcher = ItvMercuryBrandAdapter.BRAND_URL.matcher(url);
				if (matcher.matches()) {
				    return ITV_CURIE+matcher.group(1);
				}

				return null;
			}
		},
		YT {
			@Override
			public String expand(String curie) {
				return "http://www.youtube.com/watch?v=" + curie.substring(3);
			}

			@Override
			public String compact(String url) {
				return "yt:" + YoutubeUriCanonicaliser.videoIdFrom(url);
			}	
		},
		FB {
			@Override
			public String expand(String curie) {
				return "http://graph.facebook.com/" + curie.substring(3);
			}

			@Override
			public String compact(String url) {
				return "fb:" + url.replace("http://graph.facebook.com/", "");
			}	
		},
		BLIP {
			@Override
			public String expand(String curie) {
				return "http://blip.tv/file/" + curie.substring(5);
			}

			@Override
			public String compact(String url) {
				return "blip:" + removeTrailingSlash(url.replace("http://blip.tv/file/", ""));
			}
		},
		VIM {
			@Override
			public String expand(String curie) {
				return "http://vimeo.com/" + curie.substring(4);
			}

			final Pattern vimeoPattern = Pattern.compile("https?://.*vimeo.com/([^\\./&=]+).*");

			@Override
			public String compact(String url) {
				return "vim:" + matchAgainst(url, vimeoPattern);
			}	
		},
		DM {
			@Override
			public String expand(String curie) {
				return "http://www.dailymotion.com/video/" + curie.substring(3);
			}

			final Pattern dailyMotionPattern = Pattern.compile("https?://.*dailymotion.com/video/([^\\./&=]+).*");

			@Override
			public String compact(String url) {
				return "dm:" + matchAgainst(url, dailyMotionPattern);
			}	
		},
		HULU {
		    final Pattern episodePattern = Pattern.compile("^\\d+$");
		    
			@Override
			public String expand(String curie) {
			    String identifier = curie.substring(5);
			    Matcher matcher = episodePattern.matcher(identifier);
			    if (matcher.matches()) {
			        return "http://www.hulu.com/watch/" + identifier;
			    } else {
			        return "http://www.hulu.com/" + identifier;
			    }
			}

			final Pattern huluEpisodePattern = Pattern.compile("https?://.*hulu.com/watch/([^\\./&=]+).*");
			final Pattern huluBrandPattern = Pattern.compile("https?://.*hulu.com/([a-z\\-]+).*");

			@Override
			public String compact(String url) {
			    Matcher matcher = huluEpisodePattern.matcher(url);
			    if (matcher.matches()) {
			        return "hulu:" + matcher.group(1);
			    } else {
			        return "hulu:" + matchAgainst(url, huluBrandPattern);
			    }
			}	
		},
		TED {
			@Override
			public String expand(String curie) {
				return "http://www.ted.com/talks/" + curie.substring(4) + ".html";
			}

			final Pattern tedPattern = Pattern.compile("https?://.*ted.com/talks/([^\\./&=]+).html");

			@Override
			public String compact(String url) {
				return "ted:" + matchAgainst(url, tedPattern);
			}	
		},
		
		WIKI {
			
			private final Pattern WIKI_CURIE_PATTERN = Pattern.compile(name().toLowerCase() + "(:[a-z]{2})?:(.+)");

			@Override
			public String expand(String curie) {
				Matcher matcher = WIKI_CURIE_PATTERN.matcher(curie);
				if (matcher.matches()) {
					String subdomain = matcher.group(1) == null ? "en" : matcher.group(1).substring(1);
					return String.format("http://%s.wikipedia.org/%s", subdomain, matcher.group(2));
				}
				return null;
			}

			private final Pattern WIKI_FULL_URL_PATTERN = Pattern.compile("https?://([a-z]{2})\\.wikipedia.org/([^/]+)");

			@Override
			public String compact(String url) {
				Matcher matcher = WIKI_FULL_URL_PATTERN.matcher(url);
				if (matcher.matches()) {
					return String.format("%s:%s:%s", name().toLowerCase(), matcher.group(1), matcher.group(2));
				}
				return null;
			}	
		},
		
		WS_B {

		    private final Pattern WS_CURIE_PATTERN = Pattern.compile("ws-b:(\\d+)");
		    
            @Override
            public String expand(String curie) {
                Matcher matcher = WS_CURIE_PATTERN.matcher(curie);
                if (matcher.matches()) {
                    return String.format("http://wsarchive.bbc.co.uk/brands/%s", matcher.group(1));
                }
                return null;
            }

            private final Pattern WS_FULL_PATTERN = Pattern.compile("http://wsarchive.bbc.co.uk/brands/(\\d+)");

            @Override
            public String compact(String url) {
                Matcher matcher = WS_FULL_PATTERN.matcher(url);
                if (matcher.matches()) {
                    return String.format("ws-b:%s", matcher.group(1));
                }
                return null;
            }
		    
		},
		
        WS_E {

            private final Pattern WS_CURIE_PATTERN = Pattern.compile("ws-e:(\\d+)");

            @Override
            public String expand(String curie) {
                Matcher matcher = WS_CURIE_PATTERN.matcher(curie);
                if (matcher.matches()) {
                    return String.format("http://wsarchive.bbc.co.uk/episodes/%s", matcher.group(1));
                }
                return null;
            }

            private final Pattern WS_FULL_PATTERN = Pattern.compile("http://wsarchive.bbc.co.uk/episodes/(\\d+)");

            @Override
            public String compact(String url) {
                Matcher matcher = WS_FULL_PATTERN.matcher(url);
                if (matcher.matches()) {
                    return String.format("ws-e:%s", matcher.group(1));
                }
                return null;
            }

        },
        
        REDUX {

            private final Pattern REDUX_CURIE_PATTERN = Pattern.compile("redux:(\\d+)");

            @Override
            public String expand(String curie) {
                Matcher matcher = REDUX_CURIE_PATTERN.matcher(curie);
                if (matcher.matches()) {
                    return String.format("http://g.bbcredux.com/programme/%s", matcher.group(1));
                }
                return null;
            }

            private final Pattern REDUX_FULL_PATTERN = Pattern.compile("http://g.bbcredux.com/programme/(\\d+)");

            @Override
            public String compact(String url) {
                Matcher matcher = REDUX_FULL_PATTERN.matcher(url);
                if (matcher.matches()) {
                    return String.format("redux:%s", matcher.group(1));
                }
                return null;
            }
            
        },
        
        BTFEATURED {
            public final String  BTFEATURED_CURIE = "btfeatured:"; // Matches curie in BTFeaturedElementHandler
            public final String  BTFEATURED_URL = "http://featured.bt.com/products/"; // Matches uri in BTFeaturedElementHandler
            
            private final Pattern BTFEATURED_CURIE_PATTERN = Pattern.compile(BTFEATURED_CURIE+"(\\d+)");
            
            @Override
            public String expand(String curie) {
                Matcher matcher = BTFEATURED_CURIE_PATTERN.matcher(curie);
                if (matcher.matches()) {
                    return BTFEATURED_URL+matcher.group(1);
                }
                return null;
            }

            private final Pattern BTFEATURED_URL_PATTERN = Pattern.compile(BTFEATURED_URL+"(\\d+)");

            @Override
            public String compact(String url) {
                Matcher matcher = BTFEATURED_URL_PATTERN.matcher(url);
                if (matcher.matches()) {
                    return BTFEATURED_CURIE+matcher.group(1);
                }
                return null;
            }
            
        }, 
        
        PA {
            
            private final Pattern PA_CURIE_PATTERN = Pattern.compile("pa:(b|s|f|e)-([\\d-]+)");
            
            @Override
            public String expand(String curie) {
                Matcher matcher = PA_CURIE_PATTERN.matcher(curie);
                if (matcher.matches()) {
                    return String.format("http://pressassociation.com/%s/%s", shortLongTypeMap.get(matcher.group(1)), matcher.group(2));
                }
                return null;
            }
            
            private final BiMap<String,String> shortLongTypeMap = ImmutableBiMap.<String,String>builder()
                    .put("b", "brands")
                    .put("s", "series")
                    .put("f", "films")
                    .put("e", "episodes")
                    .build();

            private final Pattern PA_FULL_PATTERN = Pattern.compile("http://pressassociation.com/(brands|series|films|episodes)/([\\d-]+)");

            @Override
            public String compact(String url) {
                Matcher matcher = PA_FULL_PATTERN.matcher(url);
                if (matcher.matches()) {
                    return String.format("pa:%s-%s", shortLongTypeMap.inverse().get(matcher.group(1)), matcher.group(2));
                }
                return null;
            }
        };
		
		public abstract String expand(String curie);
		public abstract String compact(String url);
		
		private static String removeTrailingSlash(String url) {
			if (url.endsWith("/")) {
				return url.substring(0, url.length() - 1);
			}
			return url;
		}
		
		private static String matchAgainst(String uri, Pattern pattern) {
			Matcher matcher = pattern.matcher(uri);
			if (matcher.matches()) {
				return matcher.group(1);
			}
			return null;
		}

	}
	
	@Override
	public Maybe<String> expand(String curie) {
		String prefix = prefixOf(curie);
		if (prefix == null) {
			return Maybe.nothing();
		}
		CurieAlgorithm algorithm;
		try {
			algorithm = CurieAlgorithm.valueOf(prefix.toUpperCase().replace("-", "_"));
		} catch (IllegalArgumentException e) {
			// no matching algorithm
			return Maybe.nothing();
		}
		return Maybe.fromPossibleNullValue(algorithm.expand(curie));
	}

	private String prefixOf(String curie) {
		int index = curie.indexOf(":");
		if (index < 1) {
			return null;
		}
		return curie.substring(0, index);
	}
	
	private static final String C4_ATOZ_CURIE_PREFIX = "c4:atoz_";
    private static final String C4_ATOZ_URI_PREFIX = "http://www.channel4.com/programmes/atoz/";
    private static final Pattern ATOZ = Pattern.compile(C4_ATOZ_URI_PREFIX + "([a-z]|0-9)");

	private static Maybe<String> compactC4Uri(String uri) {
        Matcher matcher = ATOZ.matcher(uri);
        if (matcher.matches()) {
            return Maybe.just(C4_ATOZ_CURIE_PREFIX +  matcher.group(1));
        }
        return Maybe.nothing();
    }

    private static Maybe<String> expandC4Curie(String curie) {
        if (curie.startsWith(C4_ATOZ_CURIE_PREFIX)) {
            return Maybe.just(C4_ATOZ_URI_PREFIX + curie.substring(C4_ATOZ_CURIE_PREFIX.length()));
        }
        return Maybe.nothing();
    }
}
