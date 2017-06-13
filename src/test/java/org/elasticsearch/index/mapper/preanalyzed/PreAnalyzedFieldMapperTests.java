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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.preanalyzed.PreAnalyzedMapper.PreAnalyzedTokenStream;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

public class PreAnalyzedFieldMapperTests extends ESSingleNodeTestCase {

	MapperRegistry mapperRegistry;
	IndexService indexService;
	DocumentMapperParser parser;

	@Before
	public void setup() {
		indexService = createIndex("test");
		Map<String, Mapper.TypeParser> typeParsers = new HashMap<>();
		typeParsers.put(PreAnalyzedMapper.CONTENT_TYPE,
						new PreAnalyzedMapper.TypeParser());
		typeParsers.put("text", new TextFieldMapper.TypeParser());
		mapperRegistry = new MapperRegistry(typeParsers, Collections.<String, MetadataFieldMapper.TypeParser> emptyMap());
		parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(),
				indexService.getIndexAnalyzers(), null, indexService.similarityService(), mapperRegistry, null);
	}

	public void testSimple() throws Exception {
		// note: you must possibly configure your IDE / build system to copy the test resources into the build folder
		String mapping = IOUtils.toString(getClass().getResourceAsStream("/simpleMapping.json"), "UTF-8");
		byte[] docBytes = IOUtils.toByteArray(getClass().getResourceAsStream("/preanalyzedDoc.json"));
		DocumentMapper docMapper = parser.parse(null, new CompressedXContent(mapping));
		Document doc = docMapper.parse("test", "document", "1", new BytesArray(docBytes)).rootDoc();

		// Check field: "author"
		IndexableField field = doc.getField("author");
		assertNotNull(field);
		assertEquals("Anna Sewell", field.stringValue());

		// Check field: "title"

		IndexableField[] fields = doc.getFields("title");
		// "title" is a preanalyzed field that is also stored (see mapping). We
		// have to create two fields: one with the
		// pre-analyzed token stream and one with the stored value.
		assertEquals(2, fields.length);
		IndexableFieldType fieldType = fields[0].fieldType();
		assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fieldType.indexOptions());
		assertTrue(fieldType.tokenized());
		assertTrue(fieldType.storeTermVectorOffsets());
		assertTrue(fieldType.storeTermVectorPositions());
		assertFalse(fieldType.stored());
		// check the token stream
		TokenStream ts = fields[0].tokenStream(null, null);
		parsedPreanalyzedTokensCorrect(ts);

		fieldType = fields[1].fieldType();
		assertEquals(IndexOptions.NONE, fieldType.indexOptions());
		assertFalse(fieldType.tokenized());
		assertFalse(fieldType.storeTermVectorOffsets());
		assertFalse(fieldType.storeTermVectorPositions());
		assertTrue(fieldType.stored());
		assertEquals("Black Beauty ran past the bloody barn.", fields[1].stringValue());

		// End field "title"

		IndexableField yearField = doc.getField("year");
		assertNotNull(yearField);
		assertEquals(1877L, yearField.numericValue());
	}
	
	public void testCopyField() throws Exception {
		// note: you must possibly configure your IDE / build system to copy the test resources into the build folder
		String mapping = IOUtils.toString(getClass().getResourceAsStream("/copyToMapping.json"), "UTF-8");
		byte[] docBytes = IOUtils.toByteArray(getClass().getResourceAsStream("/preanalyzedDoc.json"));
		DocumentMapper docMapper = parser.parse(null, new CompressedXContent(mapping));
		Document doc = docMapper.parse("test", "document", "1", new BytesArray(docBytes)).rootDoc();

		// Check field: "author"
		IndexableField field = doc.getField("author");
		assertNotNull(field);
		assertEquals("Anna Sewell", field.stringValue());
		
		// Check field: "title_copy"

		IndexableField[] fields = doc.getFields("title_copy");
		// "title" is a preanalyzed field that is also stored (see mapping). We
		// have to create two fields: one with the
		// pre-analyzed token stream and one with the stored value.
		assertEquals(1, fields.length);
		IndexableFieldType fieldType = fields[0].fieldType();
		assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fieldType.indexOptions());
		assertTrue(fieldType.tokenized());
		assertFalse(fieldType.storeTermVectorOffsets());
		assertFalse(fieldType.storeTermVectorPositions());
		assertFalse(fieldType.stored());
		// check the token stream
		TokenStream ts = fields[0].tokenStream(null, null);
		parsedPreanalyzedTokensCorrect(ts);

		// End field "title"

		IndexableField yearField = doc.getField("year");
		assertNotNull(yearField);
		assertEquals(1877L, yearField.numericValue());
	}

	private void parsedPreanalyzedTokensCorrect(TokenStream ts) throws IOException {
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
		PositionIncrementAttribute posIncrAtt = ts.addAttribute(PositionIncrementAttribute.class);
		assertTrue(ts.incrementToken());
		assertEquals("Black", termAtt.toString());
		assertEquals(0, offsetAtt.startOffset());
		assertEquals(5, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("hero", termAtt.toString());
		assertEquals(0, offsetAtt.startOffset());
		assertEquals(12, offsetAtt.endOffset());
		assertEquals(0, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("Beauty", termAtt.toString());
		assertEquals(6, offsetAtt.startOffset());
		assertEquals(12, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("ran", termAtt.toString());
		assertEquals(13, offsetAtt.startOffset());
		assertEquals(16, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("past", termAtt.toString());
		assertEquals(17, offsetAtt.startOffset());
		assertEquals(21, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("the", termAtt.toString());
		assertEquals(22, offsetAtt.startOffset());
		assertEquals(25, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("bloody", termAtt.toString());
		assertEquals(26, offsetAtt.startOffset());
		assertEquals(32, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("NP", termAtt.toString());
		assertEquals(26, offsetAtt.startOffset());
		assertEquals(37, offsetAtt.endOffset());
		assertEquals(0, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("NNP", termAtt.toString());
		assertEquals(26, offsetAtt.startOffset());
		assertEquals(37, offsetAtt.endOffset());
		assertEquals(0, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals("barn", termAtt.toString());
		assertEquals(33, offsetAtt.startOffset());
		assertEquals(37, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());

		assertTrue(ts.incrementToken());
		assertEquals(".", termAtt.toString());
		assertEquals(37, offsetAtt.startOffset());
		assertEquals(38, offsetAtt.endOffset());
		assertEquals(1, posIncrAtt.getPositionIncrement());
	}

	public void testPreAnalyzedTokenStream() throws IOException {
		XContentBuilder tsBuilder = jsonBuilder().startObject().field("v", "1")
				.field("str", "This string should be stored.").startArray("tokens");
		tsBuilder.startObject().field("t", "testterm1").field("s", 1).field("e", 8).endObject();
		tsBuilder.startObject().field("t", "testterm2").field("s", 1).field("e", 8).field("i", 0).endObject();
		tsBuilder.startObject().field("t", "testterm3").field("s", 9).field("e", 15).endObject();
		tsBuilder.startObject().field("t", "testterm4").field("p", Base64.getEncoder().encodeToString("my payload".getBytes(StandardCharsets.UTF_8)))
				.field("y", "testtype").field("f", "0x4").endObject();
		
		tsBuilder.endArray().endObject();
		XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, tsBuilder.bytes(), XContentType.JSON);
		parser.nextToken(); // begin object
		parser.nextToken();
		assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken()); // "v"
		assertEquals("v", parser.currentName());
		parser.nextToken();
		assertEquals(XContentParser.Token.VALUE_STRING, parser.currentToken());
		assertEquals("1", parser.text());
		parser.nextToken();
		assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken()); // "str"
		assertEquals("str", parser.currentName());
		parser.nextToken();
		assertEquals(XContentParser.Token.VALUE_STRING, parser.currentToken());
		assertEquals("This string should be stored.", parser.text());
		parser.nextToken();
		assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken()); // "tokens"
		// This is it: We are currently at the token property. Now proceed one
		// more time. Then we are at the exact
		// position the token stream expects.
		parser.nextToken();

		try (final PreAnalyzedTokenStream ts = new PreAnalyzedMapper.PreAnalyzedTokenStream(parser)) {

			CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
			OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
			PositionIncrementAttribute posIncrAtt = ts.addAttribute(PositionIncrementAttribute.class);
			PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
			TypeAttribute typeAtt = ts.addAttribute(TypeAttribute.class);
			FlagsAttribute flagsAtt = ts.addAttribute(FlagsAttribute.class);

			assertTrue(ts.incrementToken());
			assertEquals("testterm1", new String(termAtt.buffer(), 0, termAtt.length()));
			assertEquals(1, offsetAtt.startOffset());
			assertEquals(8, offsetAtt.endOffset());
			assertEquals(1, posIncrAtt.getPositionIncrement());

			assertTrue(ts.incrementToken());
			assertEquals("testterm2", termAtt.toString());
			assertEquals(1, offsetAtt.startOffset());
			assertEquals(8, offsetAtt.endOffset());
			assertEquals(0, posIncrAtt.getPositionIncrement());

			assertTrue(ts.incrementToken());
			assertEquals("testterm3", termAtt.toString());
			assertEquals(9, offsetAtt.startOffset());
			assertEquals(15, offsetAtt.endOffset());
			assertEquals(1, posIncrAtt.getPositionIncrement());

			assertTrue(ts.incrementToken());
			assertEquals("testterm4", termAtt.toString());
			assertEquals(0, offsetAtt.startOffset());
			assertEquals(0, offsetAtt.endOffset());
			assertEquals(1, posIncrAtt.getPositionIncrement());
			assertEquals("my payload", new String(Base64.getDecoder().decode(payloadAtt.getPayload().bytes), StandardCharsets.UTF_8));
			assertEquals(4, flagsAtt.getFlags());
			assertEquals("testtype", typeAtt.type());
		}
	}
}
