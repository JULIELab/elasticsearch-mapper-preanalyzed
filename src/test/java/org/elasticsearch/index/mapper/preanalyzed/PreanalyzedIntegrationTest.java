package org.elasticsearch.index.mapper.preanalyzed;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.*;

import java.util.Map;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.jackson.core.JsonGenerator;
import org.elasticsearch.common.jackson.core.json.JsonGeneratorImpl;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContentGenerator;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.ant.tasks.junit4.events.json.JsonByteArrayAdapter;

public class PreanalyzedIntegrationTest extends ElasticsearchIntegrationTest {

	@Override
	protected Settings nodeSettings(int nodeOrdinal) {
		return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
				.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true).build();
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
		String mapping = copyToStringFromClasspath("/simpleMapping.json");
		byte[] docBytes = copyToBytesFromClasspath("/preanalyzedDoc.json");

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
		assertEquals("keyword", titleMapping.get("search_analyzer"));

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
	}
}
