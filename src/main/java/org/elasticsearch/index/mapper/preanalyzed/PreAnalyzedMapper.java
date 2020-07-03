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

import com.fasterxml.jackson.core.JsonFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.IntervalsSource;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.*;

public class PreAnalyzedMapper extends FieldMapper {

	public static final String CONTENT_TYPE = "preanalyzed";

	public static class Defaults {

		public static final MappedFieldType FIELD_TYPE = new PreanalyzedFieldType(new TextFieldMapper.TextFieldType());

		static {
			FIELD_TYPE.freeze();
		}

	}
	
	// This builder builds the whole mapper. Especially, it builds the field
	// mappers which will parse the actual sent documents.
	public static class Builder extends FieldMapper.Builder<Builder, PreAnalyzedMapper> {

		protected Builder(String name) {
			super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
			builder = this;
		}

		@Override
		public PreAnalyzedMapper build(BuilderContext context) {
			setupFieldType(context);

			// A preanalyzed field actually consists of two fields: an analyzed
			// field with a TokenStream value parsed from the JSON in the sent
			// documents, and a not-analyzed but only stored counterpart that
			// may be used to store the original value.
			// The text field will be stored but not analyzed, tokenized or
			// anything
			// except of being stored.
			FieldType fieldTypeText = new FieldType(fieldType);
			fieldTypeText.setIndexOptions(IndexOptions.NONE);
			fieldTypeText.setTokenized(false);
			fieldTypeText.setStored(fieldType.stored());
			fieldTypeText.setStoreTermVectors(false);
			fieldTypeText.setStoreTermVectorPositions(false);
			fieldTypeText.setStoreTermVectorOffsets(false);
			fieldTypeText.setStoreTermVectorPayloads(false);
			// The indexed part inherits all properties from the actually parsed
			// fieldType with the exception of the stored property which just
			// went into the fieldTypeText fieldType above.
			// We cannot change the fieldType directly because this would just
			// switch off storage off the field completely.
			MappedFieldType fieldTypeIndexed = fieldType.clone();
			fieldTypeIndexed.setStored(false);

			return new PreAnalyzedMapper(name, fieldType, defaultFieldType, context.indexSettings(),
					multiFieldsBuilder.build(this, context), copyTo, fieldTypeText, fieldTypeIndexed);
		}

	}

	/**
	 * Parses the mapping of a preanalyzed field, e.g.
	 * 
	 * <pre>
	 * "properties" : {
	 *     "text" : {
	 *         "type" : "preanalyzed",
	 *         "search_analyzer" : "standard",
	 *         "store" : true
	 *     }
	 *  }
	 * </pre>
	 * 
	 * Since the preanalyzed field type does not define any properties of its
	 * own, parsing the mapping is completely default parsing by
	 * {@link TypeParsers#parseField(org.elasticsearch.index.mapper.FieldMapper.Builder, String, Map, org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext)}
	 * .
	 * 
	 * @author faessler
	 *
	 */
	public static class TypeParser implements Mapper.TypeParser {

		// This method parses the mapping (is a field stored? token vectors?
		// etc.), it has nothing to do with an actual
		// sent document.
		@Override
		public org.elasticsearch.index.mapper.Mapper.Builder<?, ?> parse(String name, Map<String, Object> node,
				ParserContext parserContext) throws MapperParsingException {
			PreAnalyzedMapper.Builder builder = new PreAnalyzedMapper.Builder(name);
			TypeParsers.parseTextField(builder, name, node, parserContext);
			return builder;
		}

	}

	public static final class PreanalyzedFieldType extends org.elasticsearch.index.mapper.StringFieldType {
            private TextFieldMapper.TextFieldType delegateType;

        public PreanalyzedFieldType(TextFieldMapper.TextFieldType delegateType) {
            this.delegateType = delegateType;
        }

        public PreanalyzedFieldType(PreanalyzedFieldType preanalyzedFieldType) {
            super(preanalyzedFieldType);
            this.delegateType = preanalyzedFieldType.delegateType;
        }

        @Override
        public PreanalyzedFieldType clone() {
            return new PreanalyzedFieldType(this);
        }

        @Override
        public void setName(String name) {
            super.setName(name);
            delegateType.setName(name);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            PreanalyzedFieldType that = (PreanalyzedFieldType) o;
            return that.delegateType.equals(delegateType);
        }

        @Override
        public int hashCode() {
            return delegateType.hashCode();
        }

        public boolean fielddata() {
            return delegateType.fielddata();
        }

        public void setFielddata(boolean fielddata) {
            delegateType.setFielddata(fielddata);
        }

        public double fielddataMinFrequency() {
            return delegateType.fielddataMinFrequency();
        }

        public void setFielddataMinFrequency(double fielddataMinFrequency) {
          delegateType.setFielddataMinFrequency(fielddataMinFrequency);
        }

        public double fielddataMaxFrequency() {
            return delegateType.fielddataMaxFrequency();
        }

        public void setFielddataMaxFrequency(double fielddataMaxFrequency) {
            delegateType.setFielddataMaxFrequency(fielddataMaxFrequency());
        }

        public int fielddataMinSegmentSize() {
            return delegateType.fielddataMinSegmentSize();
        }

        public void setFielddataMinSegmentSize(int fielddataMinSegmentSize) {
            delegateType.setFielddataMinSegmentSize(fielddataMinSegmentSize);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            return delegateType.prefixQuery(value, method, context);
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
            return delegateType.spanPrefixQuery(value, method, context);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return delegateType.existsQuery(context);
        }

        @Override
        public IntervalsSource intervals(String text, int maxGaps, boolean ordered, NamedAnalyzer analyzer) throws IOException {
            return delegateType.intervals(text, maxGaps, ordered, analyzer);
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements) throws IOException {
            return delegateType.phraseQuery(stream, slop, enablePosIncrements);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            return delegateType.multiPhraseQuery(stream, slop, enablePositionIncrements);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
            return delegateType.phrasePrefixQuery(stream, slop, maxExpansions);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return delegateType.fielddataBuilder(fullyQualifiedIndexName);
        }

        @Override
        public void checkCompatibility(MappedFieldType other, List<String> conflicts) {
            delegateType.checkCompatibility(other, conflicts);
        }
    }


	/**
	 * Besides the default field type provided by the Builder, this field type
	 * servers to store the string value that belongs to the preanalyzed token
	 * stream which is used for indexing.
	 */
	private FieldType fieldTypeText;
	private MappedFieldType fieldTypeIndexed;
	private static final JsonFactory jsonFactory;

	static {
		jsonFactory = new JsonFactory();
	}

	public PreAnalyzedMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
			Settings indexSettings, MultiFields multiFields, CopyTo copyTo, FieldType fieldTypeText,
			MappedFieldType fieldTypeIndexed) {
		super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
		this.fieldTypeText = fieldTypeText;
		this.fieldTypeIndexed = fieldTypeIndexed;
	}

	@Override
	protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
		String preAnalyzedJson = context.parser().textOrNull();
		if (null == preAnalyzedJson)
			return;

		try (XContentParser parser = new JsonXContentParser(null, new NoopDeprecationHandler(), jsonFactory.createParser(preAnalyzedJson))) {
			Tuple<PreAnalyzedStoredValue, TokenStream> valueAndTokenStream;
			try {
				valueAndTokenStream = parsePreAnalyzedFieldContents(parser);
			} catch (MapperParsingException e) {
				throw new MapperParsingException("Could not read preanalyzed field value of document", e);
			}

			// We actually create two fields: First, a TokenStream (cannot be
			// stored!) field for the analyzed part of the
			// preanalyzed field. That is done next up.
			// Further below, if the field should also be stored, we also create
			// a new, un-analyzed but stored field with
			// the same name.
			// This will give us a stored and analyzed field in the index
			// eventually.
			if (fieldType().indexOptions() != IndexOptions.NONE && fieldType().tokenized()) {
				TokenStream ts = valueAndTokenStream.v2();

				if (ts != null) {
					Field field = new Field(fieldTypeIndexed.name(), ts, fieldTypeIndexed);
					fields.add(field);
				}
			}

			PreAnalyzedStoredValue storedValue = valueAndTokenStream.v1();
			if (fieldTypeText.stored() && null != storedValue.value) {
				Field field;
				if (PreAnalyzedStoredValue.VALUE_TYPE.STRING == storedValue.type) {
					field = new Field(fieldType().name(), (String) storedValue.value, fieldTypeText);
				} else {
					field = new Field(fieldType().name(), (BytesRef) storedValue.value, fieldTypeText);
				}
				fields.add(field);
			}
		}
	}
	
	/**
	 * This is used to send all information about the mapper to places where it
	 * is used. If we wouldn't overwrite it and add the analyzers, declaring an
	 * analyzer in the mapping would have no effect despite being set in the
	 * builder.
	 */
	@Override
	protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
		super.doXContentBody(builder, includeDefaults, params);
		doXContentAnalyzers(builder, includeDefaults);
	}

	/**
	 * Parses the contents of <tt>preAnalyzedData</tt> according to the format
	 * specified by the Solr JSON PreAnalyzed field type. The format
	 * specification can be found at the link below.
	 * 
	 * @param parser The parser for the input document to be indexed.
	 * @return A tuple, containing the plain text value and a TokenStream with
	 *         the pre-analyzed tokens.
	 * @see <a href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http://
	 *      wiki.apache.org/solr/JsonPreAnalyzedParser</a>
	 */
	private Tuple<PreAnalyzedStoredValue, TokenStream> parsePreAnalyzedFieldContents(XContentParser parser) {
		try {

			Token currentToken;
			String currentFieldName = "";
			String version = null;
			PreAnalyzedStoredValue storedValue = new PreAnalyzedStoredValue();
			PreAnalyzedTokenStream ts = null;
			while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
				if (currentToken == XContentParser.Token.FIELD_NAME) {
					currentFieldName = parser.currentName();
				} else if (currentToken == XContentParser.Token.VALUE_STRING) {
					if ("v".equals(currentFieldName)) {
						version = parser.text();
						if (!"1".equals(version)) {
							throw new MapperParsingException("Version of pre-analyzed field format is \"" + version
									+ "\" which is not supported.");
						}
					} else if ("str".equals(currentFieldName)) {
						storedValue.value = parser.text();
						storedValue.type = PreAnalyzedStoredValue.VALUE_TYPE.STRING;
					} else if ("bin".equals(currentFieldName)) {
						storedValue.value = parser.binaryValue();
						storedValue.type = PreAnalyzedStoredValue.VALUE_TYPE.BINARY;
					}
				} else if ("tokens".equals(currentFieldName) && currentToken == XContentParser.Token.START_ARRAY) {
					ts = new PreAnalyzedTokenStream(parser);
				}
			}

			if (null == version) {
				throw new MapperParsingException("No version of pre-analyzed field format has been specified for field "
						+ fieldType().name());
			}

			return new Tuple<PreAnalyzedStoredValue, TokenStream>(storedValue, ts);
		} catch (IOException e) {
			throw new MapperParsingException(
					"The input document could not be parsed as a preanalyzed field value for field "
							+ fieldType().name() + ".",
					e);
		}
	}

	public static class PreAnalyzedTokenStream extends TokenStream {
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
		 * @param parser
		 *            - The whole serialized field data, including version, the
		 *            data to store and, of course, the list of tokens.
		 * @throws IOException
		 * @see <a href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http
		 *      ://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
		 */
		PreAnalyzedTokenStream(XContentParser parser) throws IOException {
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

			Token currentToken;
			Map<String, Object> tokenMap = null;
			while ((currentToken = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
				if (currentToken == Token.START_OBJECT)
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

	private static class PreAnalyzedStoredValue {
		Object value;
		VALUE_TYPE type;

		enum VALUE_TYPE {
			STRING, BINARY
		}
	}

	@Override
	protected String contentType() {
		return CONTENT_TYPE;
	}

}
