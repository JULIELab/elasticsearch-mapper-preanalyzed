/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper.preanalyzed;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.plugin.mapper.preanalyzed.MapperPreAnalyzedPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class ObjectFieldIntergrationTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MapperPreAnalyzedPlugin.class);
    }



    @SuppressWarnings("unchecked")
    public void testSimpleIndex() throws Exception {
        String mapping = IOUtils.toString(getClass().getResourceAsStream("/preanalyzedObjectFieldsMapping.json"), "UTF-8");
        byte[] docBytes = IOUtils.toByteArray(getClass().getResourceAsStream("/preanalyzedObjectsDoc.json"));

        assertAcked(client().admin().indices().prepareCreate("test").addMapping("document", mapping, XContentType.JSON));
        // Put the preanalyzed mapping
        client().admin().indices().putMapping(putMappingRequest("test").type("document").source(mapping, XContentType.JSON)).actionGet();

        index("test", "document", "1", XContentHelper.convertToJson(new BytesArray(docBytes), false, false, XContentType.JSON));
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(queryStringQuery("Black").defaultField("sentence.text")).setSize(0).setIndices("test").execute()
                .get();
        assertEquals(1l, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch("test")
                .setQuery(queryStringQuery("black").defaultField("sentence.text")).setSize(0).setIndices("test").execute()
                .get();
        assertEquals(0l, searchResponse.getHits().getTotalHits().value);

        // actually, 'Black' and 'hero' are "on top of each other", 'hero' has a
        // position increment of 0 and comes after
        // 'Black'. we need to set a phrase slop to allow a match.
        searchResponse = client().prepareSearch("test")
                .setQuery(matchPhraseQuery("sentence.text", "Black hero").analyzer("whitespace").slop(1)).setSize(0)
                .setIndices("test").execute().get();
        assertEquals(1l, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch("test")
                .setQuery(matchPhraseQuery("sentence.text", "Beauty hero").analyzer("whitespace").slop(0)).setSize(0)
                .setIndices("test").execute().get();
        assertEquals(0l, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch("test")
                .setQuery(matchPhraseQuery("sentence.text", "Beauty hero").analyzer("whitespace").slop(3)).setSize(0)
                .setIndices("test").execute().get();
        assertEquals(1l, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("sentence.text", "Black")).storedFields("sentence.text")
                .execute().actionGet();
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        SearchHit searchHit = searchResponse.getHits().getHits()[0];

        assertTrue(((String) searchHit.field("sentence.text").getValue()).startsWith("Black Beauty"));


        // paragraph queries
         searchResponse = client().prepareSearch("test")
                .setQuery(queryStringQuery("induc").defaultField("paragraph.text")).setSize(0).setIndices("test").execute()
                .get();
        assertEquals(1l, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch("test")
                .setQuery(matchPhraseQuery("paragraph.text", "okada acid").analyzer("whitespace").slop(1)).setSize(0)
                .setIndices("test").execute().get();
        assertEquals(1l, searchResponse.getHits().getTotalHits().value);
    }
}
