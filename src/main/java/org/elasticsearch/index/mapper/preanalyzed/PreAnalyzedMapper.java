package org.elasticsearch.index.mapper.preanalyzed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class PreAnalyzedMapper extends AbstractFieldMapper<Object> implements AllFieldMapper.IncludeInAll {

	public static final String CONTENT_TYPE = "preanalyzed";

	public static class Defaults {
		public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
	}

	// This builder builds the whole mapper. Especially, it builds the field
	// mappers which will parse the actual sent documents.
	public static class Builder extends AbstractFieldMapper.Builder<Builder, PreAnalyzedMapper> {

		public Builder(String name) {
			super(name, new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE));
			this.builder = this;
		}

		@Override
		public PreAnalyzedMapper build(BuilderContext context) {
			return new PreAnalyzedMapper(buildNames(context), fieldType, docValuesProvider, docValues, searchAnalyzer,
					multiFieldsBuilder.build(this, context));
		}

	}

	/**
	 * TODO adapt example
	 * 
	 * <pre>
	 * field1 : { type : "attachment" }
	 * </pre>
	 * 
	 * Or:
	 * 
	 * <pre>
	 * field1 : {
	 * type : "attachment",
	 * fields : {
	 * field1 : {type : "binary"},
	 * title : {store : "yes"},
	 * date : {store : "yes"}
	 * }
	 * }
	 * </pre>
	 */
	public static class TypeParser implements Mapper.TypeParser {

		// This method parses the mapping (is a field stored? token vectors?
		// etc.), it has nothing to do with an actual
		// sent document.
		public Mapper.Builder<Builder, PreAnalyzedMapper> parse(String name, Map<String, Object> node,
				ParserContext parserContext) throws MapperParsingException {
			PreAnalyzedMapper.Builder builder = new PreAnalyzedMapper.Builder(name);
			parseField(builder, name, node, parserContext);

			return builder;
		}
	}

	// private final String name;

	private Boolean includeInAll;

	public PreAnalyzedMapper(Names names, FieldType fieldType, DocValuesFormatProvider docValuesProvider,
			Boolean docValues, NamedAnalyzer searchAnalyzer, MultiFields multiFields) {
		super(names, 1.0f, AbstractFieldMapper.Defaults.FIELD_TYPE, docValues, null, searchAnalyzer, null,
				docValuesProvider, null, null, null, ImmutableSettings.EMPTY, multiFields, null);
		this.fieldType = fieldType;
	}

	public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
		builder.startObject(name());
		builder.field("type", CONTENT_TYPE);
		builder.endObject();
		return builder;
	}

	@Override
	protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {

		// if (context.includeInAll(includeInAll, this)) {
		// context.allEntries().addText(names.fullName(), new String(value.bytes), boost);
		// }
		// if (!fieldType().indexed() && !fieldType().stored()) {
		// context.ignoredValue(names.indexName(), new String(value.bytes));
		// return;
		// }

		Tuple<PreAnalyzedStoredValue, TokenStream> valueAndTokenStream =
				parsePreAnalyzedFieldContents(context.parser());

		// We actually create two fields: First, a TokenStream (cannot be stored!) field for the analyzed part of the
		// preanalyzed field. That is
		// done next.
		// Further below, if the field should also be stored, we also create a new, un-analyzed but stored field with
		// the same name.
		// This will give as a stored and analyzed field in the index eventually.
		if (fieldType().indexed() && fieldType().tokenized()) {
			FieldType analyzedFieldType = new FieldType(fieldType);
			// set storing of since it would just throw an error
			analyzedFieldType.setStored(false);
			TokenStream ts = valueAndTokenStream.v2();
			if (null == ts)
				throw new MapperParsingException("The preanalyzed field \"" + names.fullName()
						+ "\" is tokenized and indexed, but no preanalyzed TokenStream could be found.");
			Field field = new Field(names.indexName(), ts, analyzedFieldType);
			field.setBoost(boost);
			fields.add(field);
		}

		PreAnalyzedStoredValue storedValue = valueAndTokenStream.v1();
		if (fieldType().stored() && null != storedValue.value) {
			// Switch of indexing and tokenization for the stored value; this sort of thing should be done for the field
			// responsible for holding the actual preanalyzed TokenStream.
			FieldType storedFieldType = new FieldType(fieldType);
			storedFieldType.setTokenized(false);
			storedFieldType.setIndexed(false);
			storedFieldType.setStoreTermVectors(false);
			storedFieldType.setStoreTermVectorOffsets(false);
			storedFieldType.setStoreTermVectorPositions(false);
			storedFieldType.setStoreTermVectorPayloads(false);
			Field field;
			if (PreAnalyzedStoredValue.VALUE_TYPE.STRING == storedValue.type) {
				field = new Field(names.indexName(), (String) storedValue.value, storedFieldType);
			} else {
				field = new Field(names.indexName(), (BytesRef) storedValue.value, storedFieldType);
			}
			fields.add(field);
		}

	}

	/**
	 * Parses the contents of <tt>preAnalyzedData</tt> according to the format specified by the Solr JSON PreAnalyzed
	 * field type. The format specification can be found at the link below.
	 * 
	 * @param xContentParser
	 * @return A tuple, containing the plain text value and a TokenStream with the pre-analyzed tokens.
	 * @see <a
	 *      href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
	 */
	private Tuple<PreAnalyzedStoredValue, TokenStream> parsePreAnalyzedFieldContents(XContentParser parser) {
		try {
			Token currentToken = parser.currentToken();
			String currentFieldName = "";
			String version = null;
			PreAnalyzedStoredValue storedValue = new PreAnalyzedStoredValue();
			PreAnalyzedTokenStream ts = null;
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
						storedValue.type = PreAnalyzedStoredValue.VALUE_TYPE.BINARY;
					}
				} else if ("tokens".equals(currentFieldName) && currentToken == XContentParser.Token.START_ARRAY) {
					ts = new PreAnalyzedTokenStream(parser);
				}
			}

			if (null == version) {
				throw new MapperParsingException("No version of pre-analyzed field format has been specified.");
			}

			return new Tuple<PreAnalyzedStoredValue, TokenStream>(storedValue, ts);
		} catch (IOException e) {
			throw new MapperParsingException("The input document could not be parsed as a preanalyzed field value.", e);
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
		 * Creates a <tt>PreAnalyzedTokenStream</tt> which converts a JSON-serialization of a TokenStream to an actual
		 * TokenStream.
		 * </p>
		 * <p>
		 * The accepted JSON format is that of the Solr JsonPreAnalyzed format (see reference below).
		 * </p>
		 * 
		 * @param parser
		 *            - The whole serialized field data, including version, the data to store and, of course, the list
		 *            of tokens.
		 * @throws IOException
		 * @see <a
		 *      href="http://wiki.apache.org/solr/JsonPreAnalyzedParser">http://wiki.apache.org/solr/JsonPreAnalyzedParser</a>
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
						"The parser is expected to point to the beginning of the array of preanalyzed tokens but the current token type was " + parser
								.currentToken());

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
						currentFieldName = parser.text();
					} else if (currentToken == XContentParser.Token.VALUE_STRING) {
						if ("t".equals(currentFieldName)) {
							char[] tokenBuffer = parser.textCharacters();
							char[] bufferCopy = new char[parser.textLength()];
							System.arraycopy(tokenBuffer, 0, bufferCopy, 0, bufferCopy.length);
							tokenMap.put("t", bufferCopy);
							// termAtt.copyBuffer(tokenBuffer, parser.textOffset(), parser.textLength());
							termFound = true;
						} else if ("p".equals(currentFieldName)) {
							// since ES 1.x - at least 1.3 - we have to make a copy of the incoming BytesRef because the
							// byte[] referenced by the input is longer than the actual information, just containing
							// zeros, which can cause problems with Base64 encoding. All we do is trim the byte array to
							// its actual length.
							BytesRef inputBytes = parser.utf8Bytes();
							byte[] byteArray = new byte[inputBytes.length];
							System.arraycopy(inputBytes.bytes, 0, byteArray, 0, inputBytes.length);
							BytesRef bytesRef = new BytesRef(byteArray);
							// payloadAtt.setPayload(bytesRef);
							tokenMap.put("p", bytesRef);
						} else if ("f".equals(currentFieldName)) {
							// flagsAtt.setFlags(Integer.decode(parser.text()));
							tokenMap.put("f", Integer.decode(parser.text()));
						} else if ("y".equals(currentFieldName)) {
							// typeAtt.setType(parser.text());
							tokenMap.put("y", parser.text());
						}
					} else if (currentToken == XContentParser.Token.VALUE_NUMBER) {
						if ("s".equals(currentFieldName)) {
							start = parser.intValue();
						} else if ("e".equals(currentFieldName)) {
							end = parser.intValue();
						} else if ("i".equals(currentFieldName)) {
							// posIncrAtt.setPositionIncrement(parser.intValue());
							tokenMap.put("i", parser.intValue());
						}
					}
				}

				if (-1 != start && -1 != end) {
					tokenMap.put("s", start);
					tokenMap.put("e", end);
					// offsetAtt.setOffset(start, end);
				}

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

				// First clear all attributes for the case that some attributes
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

				if (null != start && null != end && -1 != start && -1 != end)
					offsetAtt.setOffset(start, end);

				++tokenIndex;

				// }

				// Token currentToken = parser.nextToken();
				//
				// if (termsFieldFound && currentToken != null && currentToken != XContentParser.Token.END_ARRAY) {
				//
				// // First clear all attributes for the case that some attributes
				// // are sometimes but not always specified.
				// clearAttributes();
				//
				// boolean termFound = false;
				// int start = -1;
				// int end = -1;
				// String currentFieldName = null;
				// while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
				// if (currentToken == XContentParser.Token.FIELD_NAME) {
				// currentFieldName = parser.text();
				// } else if (currentToken == XContentParser.Token.VALUE_STRING) {
				// if ("t".equals(currentFieldName)) {
				// char[] tokenBuffer = parser.textCharacters();
				// termAtt.copyBuffer(tokenBuffer, parser.textOffset(), parser.textLength());
				// termFound = true;
				// } else if ("p".equals(currentFieldName)) {
				// // since ES 1.x - at least 1.3 - we have to make a copy of the incoming BytesRef because the
				// // byte[] referenced by the input is longer than the actual information, just containing
				// // zeros, which can cause problems with Base64 encoding. All we do is trim the byte array to
				// // its actual length.
				// BytesRef inputBytes = parser.utf8Bytes();
				// byte[] byteArray = new byte[inputBytes.length];
				// System.arraycopy(inputBytes.bytes, 0, byteArray, 0, inputBytes.length);
				// BytesRef bytesRef = new BytesRef(byteArray);
				// payloadAtt.setPayload(bytesRef);
				// } else if ("f".equals(currentFieldName)) {
				// flagsAtt.setFlags(Integer.decode(parser.text()));
				// } else if ("y".equals(currentFieldName)) {
				// typeAtt.setType(parser.text());
				// }
				// } else if (currentToken == XContentParser.Token.VALUE_NUMBER) {
				// if ("s".equals(currentFieldName)) {
				// start = parser.intValue();
				// } else if ("e".equals(currentFieldName)) {
				// end = parser.intValue();
				// } else if ("i".equals(currentFieldName)) {
				// posIncrAtt.setPositionIncrement(parser.intValue());
				// }
				// }
				// }
				//
				// if (-1 != start && -1 != end)
				// offsetAtt.setOffset(start, end);
				//
				// if (!termFound) {
				// throw new IllegalArgumentException(
				// "There is at least one token object in the pre-analyzed field value where no actual term string is specified.");
				// }

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
			tokenIndex = 0;
			// parser = XContentHelper.createParser(input.bytes, 0, input.length);
			//
			// // Go to the beginning of the token array to be ready when the
			// // tokenstream is read.
			// Token token;
			// String currentField;
			// do {
			// token = parser.nextToken();
			// if (token == XContentParser.Token.FIELD_NAME) {
			// currentField = parser.text();
			// if ("tokens".equals(currentField))
			// termsFieldFound = true;
			// }
			// } while (!termsFieldFound && token != null);
		}
	}

	static private class PreAnalyzedStoredValue {
		Object value;
		VALUE_TYPE type;

		enum VALUE_TYPE {
			STRING, BINARY
		}
	}

	@Override
	public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
		// ignore this for now
	}

	@Override
	public void traverse(FieldMapperListener fieldMapperListener) {
		//
	}

	@Override
	public void traverse(ObjectMapperListener objectMapperListener) {
		//
	}

	@Override
	public void close() {
		//
	}

	@Override
	public Object value(Object value) {
		return null;
	}

	@Override
	public FieldType defaultFieldType() {
		return AbstractFieldMapper.Defaults.FIELD_TYPE;
	}

	@Override
	public FieldDataType defaultFieldDataType() {
		return null;
	}

	@Override
	protected String contentType() {
		return CONTENT_TYPE;
	}

	@Override
	public void includeInAll(Boolean includeInAll) {
		if (includeInAll != null) {
			this.includeInAll = includeInAll;
		}
	}

	@Override
	public void includeInAllIfNotSet(Boolean includeInAll) {
		if (includeInAll != null && this.includeInAll == null) {
			this.includeInAll = includeInAll;
		}
	}

	@Override
	public void unsetIncludeInAll() {
		includeInAll = false;
	}

}
