/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.plugin.mapper.preanalyzed;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.junit.internal.ArrayComparisonFailure;

/**
 *
 */
public class PreAnalyzedFieldMapper extends AbstractFieldMapper<String> implements AllFieldMapper.IncludeInAll {

	public static final String CONTENT_TYPE = "preanalyzed";

	public static class Defaults extends AbstractFieldMapper.Defaults {
		public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

		static {
			FIELD_TYPE.freeze();
		}

	}

	public static class Builder extends AbstractFieldMapper.Builder<Builder, PreAnalyzedFieldMapper> {

		public Builder(String name) {
			super(name, new FieldType(Defaults.FIELD_TYPE));
			builder = this;
		}

		public PreAnalyzedFieldMapper build(BuilderContext context) {
			PreAnalyzedFieldMapper fieldMapper = new PreAnalyzedFieldMapper(buildNames(context), boost, fieldType,
					docValues, indexAnalyzer, searchAnalyzer, postingsProvider, docValuesProvider, similarity,
					normsLoading, fieldDataSettings, context.indexSettings());
			fieldMapper.includeInAll(includeInAll);
			return fieldMapper;
		}
	}

	public static class TypeParser implements Mapper.TypeParser {

		// The parser used to parse the mapping; thus, setting whether the field
		// is tokenized, stored etc.
		public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
				throws MapperParsingException {
			PreAnalyzedFieldMapper.Builder builder = new PreAnalyzedFieldMapper.Builder(name);
			parseField(builder, name, node, parserContext);

			return builder;
		}
	}

	private Boolean includeInAll;

	private FieldType fieldTypeText;
	private FieldType fieldTypeTokenStream;

	protected PreAnalyzedFieldMapper(Names names, Float boost, FieldType fieldType, Boolean docValues,
			NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsFormat,
			DocValuesFormatProvider docValuesFormat, SimilarityProvider similarity, Loading normsLoading,
			@Nullable Settings fieldDataSettings, Settings indexSettings) {
		super(names, boost, fieldType, docValues, indexAnalyzer, searchAnalyzer, postingsFormat, docValuesFormat,
				similarity, normsLoading, fieldDataSettings, indexSettings);

		fieldTypeTokenStream = new FieldType(fieldType);
		// TokenStream fields cannot be stored. But the option can be set anyway
		// because the plain text value should be stored.
		fieldTypeTokenStream.setStored(false);

		// The text field will be stored but not analyzed, tokenized or anything
		// except of being stored.
		fieldTypeText = new FieldType(fieldType);
		fieldTypeText.setIndexed(false);
		fieldTypeText.setTokenized(false);
		fieldTypeText.setStored(true);
		fieldTypeText.setStoreTermVectors(false);
		fieldTypeText.setStoreTermVectorPositions(false);
		fieldTypeText.setStoreTermVectorOffsets(false);
		fieldTypeText.setStoreTermVectorPayloads(false);
	}

	public FieldType defaultFieldType() {
		return Defaults.FIELD_TYPE;
	}

	public FieldDataType defaultFieldDataType() {
		// Set the default field data type to string: This way the contents are
		// interpreted as if the field type would have been "string" which is
		// important for facets, for instance.
		return new FieldDataType("string");
	}

	public void includeInAll(Boolean includeInAll) {
		if (includeInAll != null) {
			this.includeInAll = includeInAll;
		}
	}

	public void includeInAllIfNotSet(Boolean includeInAll) {
		if (includeInAll != null && this.includeInAll == null) {
			this.includeInAll = includeInAll;
		}
	}

	public String value(Object value) {
		if (value == null) {
			return null;
		}
		return value.toString();
	}

	protected boolean customBoost() {
		return true;
	}

	protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
		BytesRef value = null;
		float boost = this.boost;
		if (context.externalValueSet()) {
			value = (BytesRef) context.externalValue();
		}

		if (null == value) {
			// No value given externally. Thus we expect a simple value like
			// {"text":"{\"v\":\"1\",\"str\":\"This is a text\",\"tokens\":[...]}"}
			// in the original document. We will parse out this string and then
			// give
			// it to a new parser in the following that will "see" the actual
			// JSON
			// format. Thus, there should be one value string and we are going
			// to
			// fetch it now.
			XContentParser parser = context.parser();
			XContentParser.Token token = parser.currentToken();
			// We expect a string value encoding a JSON object which contains
			// the
			// pre-analyzed data.
			if (token == XContentParser.Token.VALUE_STRING) {
				value = parser.bytes();
			}
		}

		if (context.includeInAll(includeInAll, this)) {
			context.allEntries().addText(names.fullName(), new String(value.bytes), boost);
		}
		if (!fieldType().indexed() && !fieldType().stored()) {
			context.ignoredValue(names.indexName(), new String(value.bytes));
			return;
		}

		Tuple<PreAnalyzedStoredValue, TokenStream> valueAndTokenStream = parsePreAnalyzedFieldContents(value);

		if (fieldTypeTokenStream.indexed() && fieldTypeTokenStream.tokenized()) {
			TokenStream ts = valueAndTokenStream.v2();
			if (null == ts)
				throw new MapperParsingException("The preanalyzed field \"" + names.fullName()
						+ "\" is tokenized and indexed, but no preanalyzed TokenStream could be found.");
			Field field = new Field(names.indexName(), ts, fieldTypeTokenStream);
			field.setBoost(boost);
			// context.doc().add(field);
			fields.add(field);
		}

		PreAnalyzedStoredValue storedValue = valueAndTokenStream.v1();
		if (fieldTypeText.stored() && null != storedValue.value) {
			Field field;
			if (PreAnalyzedStoredValue.VALUE_TYPE.STRING == storedValue.type) {
				field = new Field(names.indexName(), (String) storedValue.value, fieldTypeText);
			} else {
				field = new Field(names.indexName(), (BytesRef) storedValue.value, fieldTypeText);
			}
			// context.doc().add(field);
			fields.add(field);
		}

	}

	/**
	 * Parses the contents of <tt>preAnalyzedData</tt> according to the format specified by the Solr JSON PreAnalyzed
	 * field type. The format specification can be found at the link below.
	 * 
	 * @param preAnalyzedData
	 * @return A tuple, containing the plain text value and a TokenStream with the pre-analyzed tokens.
	 * @see <a
	 *      href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
	 */
	private Tuple<PreAnalyzedStoredValue, TokenStream> parsePreAnalyzedFieldContents(BytesRef preAnalyzedData) {
		try {
			XContentParser parser = XContentHelper.createParser(preAnalyzedData.bytes, 0, preAnalyzedData.length);
			Token currentToken = parser.currentToken();
			String currentFieldName = "";
			String version = null;
			PreAnalyzedStoredValue storedValue = new PreAnalyzedStoredValue();
			while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
				if (currentToken == XContentParser.Token.FIELD_NAME) {
					currentFieldName = parser.text();
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
						storedValue.type = PreAnalyzedStoredValue.VALUE_TYPE.STRING;
					}
				}
			}

			if (null == version) {
				throw new MapperParsingException("No version of pre-analyzed field format has been specified.");
			}

			return new Tuple<PreAnalyzedStoredValue, TokenStream>(storedValue, new PreAnalyzedTokenStream(
					preAnalyzedData));
		} catch (IOException e) {
			throw new MapperParsingException("The input document could not be parsed as a preanalyzed field value.", e);
		}
	}

	protected String contentType() {
		return CONTENT_TYPE;
	}

	public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
		super.merge(mergeWith, mergeContext);
		if (!this.getClass().equals(mergeWith.getClass())) {
			return;
		}
		if (!mergeContext.mergeFlags().simulate()) {
			this.includeInAll = ((PreAnalyzedFieldMapper) mergeWith).includeInAll;
		}
	}

	protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
		super.doXContentBody(builder, includeDefaults, params);
		if (includeInAll != null) {
			builder.field("include_in_all", includeInAll);
		}
	}

	static class PreAnalyzedTokenStream extends TokenStream {
		private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
		private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
		private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
		private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
		private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
		private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
		private XContentParser parser;
		private boolean termsFieldFound = false;
		private final BytesRef input;

		/**
		 * <p>
		 * Creates a <tt>PreAnalyzedTokenStream</tt> which converts a JSON-serialization of a TokenStream to an actual
		 * TokenStream.
		 * </p>
		 * <p>
		 * The accepted JSON format is that of the Solr JsonPreAnalyzed format (see reference below).
		 * </p>
		 * 
		 * @param input
		 *            - The whole serialized field data, including version, the data to store and, of course, the list
		 *            of tokens.
		 * @throws IOException
		 * @see <a
		 *      href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
		 */
		PreAnalyzedTokenStream(BytesRef input) throws IOException {
			this.input = input;
			reset();
		}

		@Override
		public final boolean incrementToken() throws IOException {
			Token currentToken = parser.nextToken();

			if (termsFieldFound && currentToken != null && currentToken != XContentParser.Token.END_ARRAY) {

				// First clear all attributes for the case that some attributes
				// are sometimes but not always specified.
				clearAttributes();

				boolean termFound = false;
				int start = -1;
				int end = -1;
				String currentFieldName = null;
				while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
					if (currentToken == XContentParser.Token.FIELD_NAME) {
						currentFieldName = parser.text();
					} else if (currentToken == XContentParser.Token.VALUE_STRING) {
						if ("t".equals(currentFieldName)) {
							char[] tokenBuffer = parser.textCharacters();
							termAtt.copyBuffer(tokenBuffer, parser.textOffset(), parser.textLength());
							termFound = true;
						} else if ("p".equals(currentFieldName)) {
							// since ES 1.x - at least 1.3 - we have to make a copy of the incoming BytesRef because the
							// byte[] referenced by the input is longer than the actual information, just containing
							// zeros, which can cause problems with Base64 encoding. All we do is trim the byte array to
							// its actual length.
							BytesRef inputBytes = parser.bytes();
							byte[] byteArray = new byte[inputBytes.length];
							System.arraycopy(inputBytes.bytes, 0, byteArray, 0, inputBytes.length);
							BytesRef bytesRef = new BytesRef(byteArray);
							payloadAtt.setPayload(bytesRef);
						} else if ("f".equals(currentFieldName)) {
							flagsAtt.setFlags(Integer.decode(parser.text()));
						} else if ("y".equals(currentFieldName)) {
							typeAtt.setType(parser.text());
						}
					} else if (currentToken == XContentParser.Token.VALUE_NUMBER) {
						if ("s".equals(currentFieldName)) {
							start = parser.intValue();
						} else if ("e".equals(currentFieldName)) {
							end = parser.intValue();
						} else if ("i".equals(currentFieldName)) {
							posIncrAtt.setPositionIncrement(parser.intValue());
						}
					}
				}

				if (-1 != start && -1 != end)
					offsetAtt.setOffset(start, end);

				if (!termFound) {
					throw new IllegalArgumentException(
							"There is at least one token object in the pre-analyzed field value where no actual term string is specified.");
				}

				return true;
			}
			return false;
		}

		/**
		 * Creates a new parser reading the input data and sets the parser state right to the beginning of the actual
		 * token list.
		 */
		@Override
		public void reset() throws IOException {
			parser = XContentHelper.createParser(input.bytes, 0, input.length);

			// Go to the beginning of the token array to be ready when the
			// tokenstream is read.
			Token token;
			String currentField;
			do {
				token = parser.nextToken();
				if (token == XContentParser.Token.FIELD_NAME) {
					currentField = parser.text();
					if ("tokens".equals(currentField))
						termsFieldFound = true;
				}
			} while (!termsFieldFound && token != null);
		}
	}

	static private class PreAnalyzedStoredValue {
		Object value;
		VALUE_TYPE type;

		enum VALUE_TYPE {
			STRING, BINARY
		}
	}

	public void unsetIncludeInAll() {
		includeInAll = false;
	}

}
