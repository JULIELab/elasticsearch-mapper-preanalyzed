package org.elasticsearch.index.mapper.preanalyzed;

import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.jackson.core.JsonParser;
import org.elasticsearch.common.jackson.core.json.ReaderBasedJsonParser;
import org.elasticsearch.common.jackson.dataformat.smile.SmileFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.ContentPath.Type;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.plugin.mapper.preanalyzed.PreAnalyzedFieldMapper;

public class PreAnalyzedMapper implements Mapper {

	public static final String CONTENT_TYPE = "preanalyzed";

	public static class Defaults {
		public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
	}

	// This builder builds the whole mapper. Especially, it builds the field
	// mappers which will parse the actual sent documents.
	public static class Builder extends
			Mapper.Builder<Builder, PreAnalyzedMapper> {

		private ContentPath.Type pathType = Defaults.PATH_TYPE;

		private PreAnalyzedFieldMapper.Builder contentBuilder;

		public Builder(String name) {
			super(name);
			this.builder = this;
		}

		public Builder pathType(ContentPath.Type pathType) {
			this.pathType = pathType;
			return this;
		}

		public Builder content(PreAnalyzedFieldMapper.Builder builder) {
			this.contentBuilder = builder;
			return this;
		}

		@Override
		public PreAnalyzedMapper build(BuilderContext context) {
			// ContentPath.Type origPathType = context.path().pathType();
			// context.path().pathType(pathType);

			// create the content mapper under the actual name
			PreAnalyzedFieldMapper contentMapper = contentBuilder
					.build(context);

			// create the DC one under the name
			// context.path().add(name);
			// context.path().remove();
			//
			// context.path().pathType(origPathType);

			return new PreAnalyzedMapper(name, pathType, contentMapper);
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
		@SuppressWarnings({ "unchecked" })
		public Mapper.Builder parse(String name, Map<String, Object> node,
				ParserContext parserContext) throws MapperParsingException {
			PreAnalyzedMapper.Builder builder = new PreAnalyzedMapper.Builder(
					name);
			boolean textFieldConfigured = false;
			for (Map.Entry<String, Object> entry : node.entrySet()) {
				String fieldName = entry.getKey();
				Object fieldNode = entry.getValue();
				if (fieldName.equals("path")) {
					builder.pathType(parsePathType(name, fieldNode.toString()));
				} else if (fieldName.equals("fields")) {
					Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
					for (Map.Entry<String, Object> entry1 : fieldsNode
							.entrySet()) {
						String propName = entry1.getKey();
						Object propNode = entry1.getValue();
						if (name.equals(propName)) {
							// that is the content
							builder.content((PreAnalyzedFieldMapper.Builder) parserContext
									.typeParser("tokenstream_json").parse(name,
											(Map<String, Object>) propNode,
											parserContext));
							textFieldConfigured = true;
						}
					}
				}
			}

			if (!textFieldConfigured)
				throw new MapperParsingException(
						"Configuration not complete, field type definition missing (only string is currently supported).");

			return builder;
		}
	}

	private final String name;

	private final ContentPath.Type pathType;

	private final PreAnalyzedFieldMapper contentMapper;

	public PreAnalyzedMapper(String name, Type pathType,
			PreAnalyzedFieldMapper contentMapper) {
		this.name = name;
		this.pathType = pathType;
		this.contentMapper = contentMapper;

	}

	public XContentBuilder toXContent(XContentBuilder builder, Params params)
			throws IOException {
		builder.startObject(name);
		builder.field("type", CONTENT_TYPE);
		builder.field("path", pathType.name().toLowerCase());

		builder.startObject("fields");
		contentMapper.toXContent(builder, params);

		builder.endObject();

		builder.endObject();
		return builder;
	}

	public String name() {
		return name;
	}

	// This method parses an actual document.
	public void parse(ParseContext context) throws IOException {
		BytesRef preAnalyzedData = null;

		XContentParser parser = context.parser();
		XContentParser.Token token = parser.currentToken();
		// We expect a string value encoding a JSON object which contains the
		// pre-analyzed data.
		if (token == XContentParser.Token.VALUE_STRING) {
			preAnalyzedData = parser.bytes();
		}

		context.externalValue(preAnalyzedData);
		contentMapper.parse(context);
	}

	public void merge(Mapper mergeWith, MergeContext mergeContext)
			throws MergeMappingException {
		// ignore this for now
	}

	public void traverse(FieldMapperListener fieldMapperListener) {
		contentMapper.traverse(fieldMapperListener);
	}

	public void traverse(ObjectMapperListener objectMapperListener) {
	}

	public void close() {
		contentMapper.close();
	}

}
