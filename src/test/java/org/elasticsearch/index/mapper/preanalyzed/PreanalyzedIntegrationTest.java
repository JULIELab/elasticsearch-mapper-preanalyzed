package org.elasticsearch.index.mapper.preanalyzed;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.plugin.mapper.preanalyzed.MapperPreAnalyzedPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PreanalyzedIntegrationTest extends ESIntegTestCase {

//	@Override
//	protected Settings nodeSettings(int nodeOrdinal) {
//		
//		return Settings.builder().put(super.nodeSettings(nodeOrdinal))
//				.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true).build();
//	}
	
	 @Override
	    protected Collection<Class<? extends Plugin>> nodePlugins() {
	        return Collections.<Class<? extends Plugin>>singleton(MapperPreAnalyzedPlugin.class);
	    }

	@Before
	public void createEmptyIndex() throws Exception {
		logger.info("creating index [test]");
		internalCluster().wipeIndices("test");
		createIndex("test");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSimpleIndex() throws Exception {
		String mapping = IOUtils.toString(getClass().getResourceAsStream("/simpleMapping.json"), "UTF-8");
		byte[] docBytes = IOUtils.toByteArray(getClass().getResourceAsStream("/preanalyzedDoc.json"));

		// Put the preanalyzed mapping check that it is there indeed
		client().admin().indices().putMapping(putMappingRequest("test").type("document").source(mapping)).actionGet();
		GetMappingsResponse actionGet =
				client().admin().indices().getMappings(new GetMappingsRequest().indices("test")).get();
		Map<String, Object> mappingProperties =
				(Map<String, Object>) actionGet.getMappings().get("test").get("document").getSourceAsMap()
						.get("properties");
		assertTrue(mappingProperties.keySet().contains("title"));
		Map<String, Object> titleMapping = (Map<String, Object>) mappingProperties.get("title");
		assertEquals("preanalyzed", titleMapping.get("type"));
		assertEquals(true, titleMapping.get("store"));
		assertEquals("with_positions_offsets", titleMapping.get("term_vector"));
		assertEquals("keyword", titleMapping.get("analyzer"));

		index("test", "document", "1", XContentHelper.convertToJson(docBytes, 0, docBytes.length, false));
		refresh();

		CountResponse countResponse =
				client().prepareCount("test").setQuery(queryStringQuery("Black").defaultField("title")).execute().get();
		assertEquals(1l, countResponse.getCount());

		countResponse =
				client().prepareCount("test").setQuery(queryStringQuery("black").defaultField("title")).execute().get();
		assertEquals(0l, countResponse.getCount());

		// actually, 'Black' and 'hero' are "on top of each other", 'hero' has a position increment of 0 and comes after
		// 'Black'. we need to set a phrase slop to allow a match.
		countResponse =
				client().prepareCount("test")
						.setQuery(matchPhraseQuery("title", "Black hero").analyzer("whitespace").slop(1)).execute()
						.get();
		assertEquals(1l, countResponse.getCount());

		countResponse =
				client().prepareCount("test")
						.setQuery(matchPhraseQuery("title", "Beauty hero").analyzer("whitespace").slop(0)).execute()
						.get();
		assertEquals(0l, countResponse.getCount());

		countResponse =
				client().prepareCount("test")
						.setQuery(matchPhraseQuery("title", "Beauty hero").analyzer("whitespace").slop(3)).execute()
						.get();
		assertEquals(1l, countResponse.getCount());

		countResponse =
				client().prepareCount("test").setQuery(queryStringQuery("Anne Sewell").defaultField("author"))
						.execute().get();
		assertEquals(1l, countResponse.getCount());

		countResponse =
				client().prepareCount("test").setQuery(queryStringQuery("1877").defaultField("year")).execute().get();
		assertEquals(1l, countResponse.getCount());
		
		SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("title", "Black")).addField("title").execute().actionGet();
		assertEquals(1, searchResponse.getHits().getTotalHits());
		SearchHit searchHit = searchResponse.getHits().getHits()[0];
		assertTrue(((String)searchHit.field("title").value()).startsWith("Black Beauty"));
	}
}
