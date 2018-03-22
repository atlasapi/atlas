package org.atlasapi.views;

import org.springframework.core.Ordered;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;

import java.util.Locale;

/**
 * View resolver that always returns a fixed {@link View}.
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class SingleViewViewResolver implements ViewResolver, Ordered {

	private final View view;

	private int order;

	private String mappedView;

	public SingleViewViewResolver(View view) {
		this.view = view;
	}

	@Override
	public View resolveViewName(String viewName, Locale locale) throws Exception {
		if (mappedView.equals(viewName)) { return view; }
		return null;
	}

	@Override
	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}
	
	public void setMappedView(String mappedView) {
		this.mappedView = mappedView;
	}

}
