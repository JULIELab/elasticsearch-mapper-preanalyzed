package org.elasticsearch.index.plugin.mapper.preanalyzed;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.plugin.mapper.preanalyzed.PreAnalyzedFieldMapper.PreAnalyzedTokenStream;
import org.junit.Test;

public class PreAnalyzedFieldMapperTest {
	@Test
	public void testPreAnalyzedTokenStream() throws IOException {
		XContentBuilder tsBuilder = jsonBuilder().startObject().field("v", "1")
				.field("str", "This string should be stored.")
				.startArray("tokens");
		tsBuilder.startObject().field("t", "testterm1").field("s", 1)
				.field("e", 8).endObject();
		tsBuilder.startObject().field("t", "testterm2").field("s", 1)
				.field("e", 8).field("i", 0).endObject();
		tsBuilder.startObject().field("t", "testterm3").field("s", 9)
				.field("e", 15).endObject();
		tsBuilder.startObject().field("t", "testterm4")
				.field("p", Base64.encodeBytes("my payload".getBytes()))
				.field("y", "testtype").field("f", "0x4").endObject();
		tsBuilder.endArray().endObject();

		PreAnalyzedTokenStream ts = new PreAnalyzedFieldMapper.PreAnalyzedTokenStream(
				tsBuilder.bytes().toBytesRef());

		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
		PositionIncrementAttribute posIncrAtt = ts
				.addAttribute(PositionIncrementAttribute.class);
		PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
		TypeAttribute typeAtt = ts.addAttribute(TypeAttribute.class);
		FlagsAttribute flagsAtt = ts.addAttribute(FlagsAttribute.class);

		assertTrue(ts.incrementToken());
		assertEquals("testterm1",
				new String(termAtt.buffer(), 0, termAtt.length()));
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
