package org.elasticsearch.index.plugin.mapper.preanalyzed;

import java.util.Collection;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class MapperPreAnalyzedPlugin extends AbstractPlugin {

	public String name() {
		return "mapper-preanalyzed";
	}

	public String description() {
		return "Allows to index pre-analyzed field contents.";
	}

	public Collection<Class<? extends Module>> indexModules() {
		Collection<Class<? extends Module>> modules = newArrayList();
		modules.add(PreAnalyzedIndexModule.class);
		return modules;
	}
}
