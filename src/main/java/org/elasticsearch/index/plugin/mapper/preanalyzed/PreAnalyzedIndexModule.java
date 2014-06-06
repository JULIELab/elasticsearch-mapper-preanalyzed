package org.elasticsearch.index.plugin.mapper.preanalyzed;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.mapper.preanalyzed.RegisterPreAnalyzedType;

public class PreAnalyzedIndexModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(RegisterPreAnalyzedType.class).asEagerSingleton();
	}

}
