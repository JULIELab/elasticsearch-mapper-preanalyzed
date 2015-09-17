package org.elasticsearch.index.mapper.preanalyzed;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.preanalyzed.PreAnalyzedMapper.PreAnalyzedTokenStream;
import org.junit.Before;
import org.junit.Test;

public class PreAnalyzedFieldMapperTest {

	private DocumentMapperParser mapperParser;

	@Before
	public void setupMapperParser() {
		mapperParser = MapperTestUtils.newMapperParser();
		// register the preanalyzed mapper among the available mappers
		mapperParser.putTypeParser(PreAnalyzedMapper.CONTENT_TYPE, new PreAnalyzedMapper.TypeParser());
	}

	@SuppressWarnings("resource")
	@Test
	public void testSimple() throws Exception {
		String mapping = copyToStringFromClasspath("/simpleMapping.json");
		DocumentMapper docMapper = mapperParser.parse(mapping);
		byte[] docBytes = copyToBytesFromClasspath("/preanalyzedDoc.json");
		BytesStreamInput input = new BytesStreamInput(docBytes);
		BytesReference bytesRef = input.readBytesReference(docBytes.length);
		ParseContext.Document doc = docMapper.parse(bytesRef).rootDoc();
		
		// Check field: "author"
		IndexableField field = doc.getField("author");
		assertNotNull(field);
		assertEquals("Anna Sewell", field.stringValue());
		
		// Check field: "title"
		
		IndexableField[] fields = doc.getFields("title");
		// "title" is a preanalyzed field that is also stored (see mapping). We have to create two fields: one with the
		// pre-analyzed token stream and one with the stored value.
		assertEquals(2, fields.length);
		IndexableFieldType fieldType = fields[0].fieldType();
		assertTrue(fieldType.indexed());
		assertTrue(fieldType.tokenized());
		assertTrue(fieldType.storeTermVectorOffsets());
		assertTrue(fieldType.storeTermVectorPositions());
		assertFalse(fieldType.stored());
		// check the token stream
		TokenStream ts = fields[0].tokenStream(null, null);
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

		fieldType = fields[1].fieldType();
		assertFalse(fieldType.indexed());
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

	@Test
	public void testPreAnalyzedTokenStream() throws IOException {
		XContentBuilder tsBuilder =
				jsonBuilder().startObject().field("v", "1").field("str", "This string should be stored.")
						.startArray("tokens");
		tsBuilder.startObject().field("t", "testterm1").field("s", 1).field("e", 8).endObject();
		tsBuilder.startObject().field("t", "testterm2").field("s", 1).field("e", 8).field("i", 0).endObject();
		tsBuilder.startObject().field("t", "testterm3").field("s", 9).field("e", 15).endObject();
		tsBuilder.startObject().field("t", "testterm4").field("p", Base64.encodeBytes("my payload".getBytes()))
				.field("y", "testtype").field("f", "0x4").endObject();
		tsBuilder.endArray().endObject();
		XContentParser parser = XContentHelper.createParser(tsBuilder.bytesStream().bytes());
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
		// This is it: We are currently at the token property. Now proceed one more time. Then we are at the exact
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
			assertEquals("my payload", new String(Base64.decode(payloadAtt.getPayload().bytes)));
			assertEquals(4, flagsAtt.getFlags());
			assertEquals("testtype", typeAtt.type());
		}
	}
}
