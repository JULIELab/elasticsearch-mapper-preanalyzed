package org.elasticsearch.index.mapper.preanalyzed;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PreanalyzedTokenStream extends TokenStream {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
    private XContentParser parser;
    private ArrayList<Map<String, Object>> tokenList;
    private int tokenIndex;

    /**
     * <p>
     * Creates a <tt>PreAnalyzedTokenStream</tt> which converts a
     * JSON-serialization of a TokenStream to an actual TokenStream.
     * </p>
     * <p>
     * The accepted JSON format is that of the Solr JsonPreAnalyzed format
     * (see reference below).
     * </p>
     *
     * @param parser - The whole serialized field data, including version, the
     *               data to store and, of course, the list of tokens.
     * @throws IOException
     * @see <a href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http
     * ://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
     */
    public PreanalyzedTokenStream(XContentParser parser) throws IOException {
        this.parser = parser;
        parsePreanalyzedTokens();
        reset();
    }

    private void parsePreanalyzedTokens() throws NumberFormatException, IOException {
        tokenList = new ArrayList<>();
        if (parser.currentToken() != XContentParser.Token.START_ARRAY)
            throw new IllegalStateException(
                    "The parser is expected to point to the beginning of the array of preanalyzed tokens but the current token type was "
                            + parser.currentToken());

        XContentParser.Token currentToken;
        Map<String, Object> tokenMap = null;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (currentToken == XContentParser.Token.START_OBJECT)
                tokenMap = new HashMap<>();
            // First clear all attributes for the case that some attributes
            // are sometimes but not always specified.
            clearAttributes();

            boolean termFound = false;
            int start = -1;
            int end = -1;
            String currentFieldName = null;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentToken == XContentParser.Token.VALUE_STRING) {
                    if ("t".equals(currentFieldName)) {
                        char[] tokenBuffer = parser.textCharacters();
                        char[] bufferCopy = new char[parser.textLength()];
                        System.arraycopy(tokenBuffer, parser.textOffset(), bufferCopy, 0, bufferCopy.length);
                        tokenMap.put("t", bufferCopy);
                        termFound = true;
                    } else if ("p".equals(currentFieldName)) {
                        // since ES 1.x - at least 1.3 - we have to make a
                        // copy of the incoming BytesRef because the
                        // byte[] referenced by the input is longer than the
                        // actual information, just containing
                        // zeros, which can cause problems with Base64
                        // encoding. All we do is trim the byte array to
                        // its actual length.
//							BytesRef inputBytes = parser.utf8Bytes();
//							byte[] byteArray = new byte[inputBytes.length];
//							System.arraycopy(inputBytes.bytes, 0, byteArray, 0, inputBytes.length);
                        byte[] byteArray = parser.charBuffer().toString().getBytes("UTF-8");
                        BytesRef bytesRef = new BytesRef(byteArray);
                        tokenMap.put("p", bytesRef);
                    } else if ("f".equals(currentFieldName)) {
                        tokenMap.put("f", Integer.decode(parser.text()));
                    } else if ("y".equals(currentFieldName)) {
                        tokenMap.put("y", parser.text());
                    }
                } else if (currentToken == XContentParser.Token.VALUE_NUMBER) {
                    if ("s".equals(currentFieldName)) {
                        start = parser.intValue();
                    } else if ("e".equals(currentFieldName)) {
                        end = parser.intValue();
                    } else if ("i".equals(currentFieldName)) {
                        tokenMap.put("i", parser.intValue());
                    }
                }
            }

            tokenMap.put("s", start != -1 ? start : 0);
            tokenMap.put("e", end != -1 ? end : 0);

            if (!termFound) {
                throw new IllegalArgumentException(
                        "There is at least one token object in the pre-analyzed field value where no actual term string is specified.");
            }
            tokenList.add(tokenMap);
        }
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (tokenIndex < tokenList.size()) {
            Map<String, Object> t = tokenList.get(tokenIndex);
            char[] termChars = (char[]) t.get("t");
            BytesRef payload = (BytesRef) t.get("p");
            Integer flags = (Integer) t.get("f");
            String type = (String) t.get("y");
            Integer start = (Integer) t.get("s");
            Integer end = (Integer) t.get("e");
            Integer posInc = (Integer) t.get("i");

            try {
                // First clear all attributes for the case that some
                // attributes
                // are sometimes but not always specified.
                clearAttributes();

                if (null != termChars)
                    termAtt.copyBuffer(termChars, 0, termChars.length);
                if (null != payload)
                    payloadAtt.setPayload(payload);
                if (null != flags)
                    flagsAtt.setFlags(flags);
                if (null != type)
                    typeAtt.setType(type);
                if (null != posInc)
                    posIncrAtt.setPositionIncrement(posInc);

                if (null != start && null != end && -1 != start && -1 != end) {
                    offsetAtt.setOffset(start, end);

                    ++tokenIndex;

                    return true;
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception occurred at token term: " + new String(termChars)
                        + ", start: " + start + ", end: " + end + ", positionIncrement: " + posInc, e);
            }
        }
        return false;
    }

    /**
     * Creates a new parser reading the input data and sets the parser state
     * right to the beginning of the actual token list.
     */
    @Override
    public void reset() throws IOException {
        tokenIndex = 0;
    }
}
