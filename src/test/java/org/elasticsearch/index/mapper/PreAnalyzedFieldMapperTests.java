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
package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.preanalyzed.NoopDeprecationHandler;
import org.elasticsearch.index.mapper.preanalyzed.PreanalyzedTokenStream;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;


public class PreAnalyzedFieldMapperTests extends MapperTestCase {

	@Override
	protected Collection<? extends Plugin> getPlugins() {
		return List.of(new MapperPreanalyzedPlugin());
	}

	public void testSimple() throws Exception {
		MapperService mapperService = createMapperService(fieldMapping(b -> {
			minimalMapping(b);
			b.field("store", true);
				b.field("term_vector", "with_positions_offsets");
		}));
		final DocumentMapper docMapper = mapperService.documentMapper();
		ParsedDocument doc = docMapper.parse(source(b -> b.field("field", getSampleValueForDocument())));


		IndexableField[] fields = doc.rootDoc().getFields("field");
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
		XContentType xContentType = XContentType.JSON;
		XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, new NoopDeprecationHandler(), Strings.toString(tsBuilder));
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

		try (final PreanalyzedTokenStream ts = new PreanalyzedTokenStream(parser)) {

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


	@Override
	protected void minimalMapping(XContentBuilder xContentBuilder) throws IOException {
		xContentBuilder.field("type", PreanalyzedFieldMapper.CONTENT_TYPE);
	}

	@Override
	protected Object getSampleValueForDocument() {
		return "{\"v\":\"1\",\"str\":\"Black Beauty ran past the bloody barn.\",\"tokens\":[{\"t\":\"Black\",\"s\":0,\"e\":5,\"i\":1},{\"t\":\"hero\",\"s\":0,\"e\":12,\"i\":0},{\"t\":\"Beauty\",\"s\":6,\"e\":12,\"i\":1},{\"t\":\"ran\",\"s\":13,\"e\":16,\"i\":1},{\"t\":\"past\",\"s\":17,\"e\":21,\"i\":1},{\"t\":\"the\",\"s\":22,\"e\":25,\"i\":1},{\"t\":\"bloody\",\"s\":26,\"e\":32,\"i\":1},{\"t\":\"NP\",\"s\":26,\"e\":37,\"i\":0},{\"t\":\"NNP\",\"s\":26,\"e\":37,\"i\":0},{\"t\":\"barn\",\"s\":33,\"e\":37,\"i\":1},{\"t\":\".\",\"s\":37,\"e\":38,\"i\":1}]}";
	}

	@Override
	protected void registerParameters(ParameterChecker checker) throws IOException {
		// needed to avoid an error in testUpdates()
		checker.registerUpdateCheck(b -> b.field("boost", 2.0), m -> assertEquals(m.fieldType().boost(), 2.0, 0));
	}

	@Override
	protected Object generateRandomInputValue(MappedFieldType mappedFieldType) {
		return null;
	}

	/**
	 * This basically disables tests with multi-field values that would raise an IllegalArgumentException because
	 * there is no fielddata.
	 * @param b
	 * @throws IOException
	 */
	@Override
	protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
		assumeFalse("We don't have a way to assert things here", true);
	}

	/**
	 * This is required to affirm that the warning in the testMinimalToMaximal test is expected.
	 * @return
	 */
	@Override
	protected String[] getParseMaximalWarnings() {
		return new String[] { "Parameter [boost] on field [field] is deprecated and will be removed in 8.0" };
	}



}
