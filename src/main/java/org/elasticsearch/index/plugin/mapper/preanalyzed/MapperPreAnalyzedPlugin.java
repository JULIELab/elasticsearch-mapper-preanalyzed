package org.elasticsearch.index.plugin.mapper.preanalyzed;

import org.elasticsearch.index.mapper.preanalyzed.PreAnalyzedMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;

public class MapperPreAnalyzedPlugin extends Plugin {

	@Override
	public String name() {
		return "mapper-preanalyzed";
	}

	@Override
	public String description() {
		return "Allows to index pre-analyzed field contents.";
	}

	public void onModule(IndicesModule indicesModule) {
		indicesModule.registerMapper("preanalyzed", new PreAnalyzedMapper.TypeParser());
	}

}
