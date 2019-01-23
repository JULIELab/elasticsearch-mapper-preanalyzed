elasticsearch-mapper-preanalyzed
================================

An ElasticSearch mapper plugin that allows to index preanalyzed TokenStreams, i.e. to circumvent an index analyzer in ElasticSearch and instead exactly specifying each single token to be indexed.

ElasticSearch compatibility table:

| elasticsearch |  Preanalyzed Mapper Plugin | Docs
|---------------|----------------------------|------
| es-5.4.3      |  5.4.3 | [5.4.3-mvn](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.4.3-mvn)
| es-5.4.3      |  5.4.3 | [5.4.3](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.4.3)
| es-5.4.1      |  5.4.1 | [5.4.1](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.4.1)
| es-5.4.0      |  5.4.0 | [5.4.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.4.0)
| es-5.3.3      |  5.3.3 | [5.3.3-mvn](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.3.3-mvn)
| es-5.3.2      |  5.3.2 | [5.3.2](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.3.2)
| es-5.3.1      |  5.3.1 | [5.3.1](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.3.1)
| es-5.3.0      |  5.3.0 | [5.3.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.3.0)
| es-5.2.2      |  5.2.2 | [5.2.2](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.2.2)
| es-5.0.2      |  5.0.2 | [5.0.2](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.0.2)
| es-5.0.0      |  5.0.0 | [5.0.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-5.0.0)
| es-2.4.6      |  2.4.6 | [2.4.6](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.4.6)
| es-2.4.0      |  2.4.0 | [2.4.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.4.0)
| es-2.3.5      |  2.3.5 | [2.3.5](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.3.5)
| es-2.3.0      |  2.3.0 | [2.3.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.3.0)
| es-2.2.2      |  2.2.2 | [2.2.2](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.2.2)
| es-2.2.0      |  2.2.0 | [2.2.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.2.0)
| es-2.1.2      |  2.1.2 | [2.1.2](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.1.2)
| es-2.1.1      |  2.1.1 | [2.1.1](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-2.1.1)
| es-1.7		|  0.1.0 | [0.1.0](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-1.7)
| es-1.5        |  0.0.5 | [0.0.5](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-1.5)
| es-1.4        |  0.0.4 | [0.0.4](https://github.com/JULIELab/elasticsearch-mapper-preanalyzed/tree/es-1.4)

This can be useful if a stand-alone text analysis engine is available, for instance a UIMA or GATE pipeline, and you would like to transfer the analysis results one to one into the ElasticSearch index.
For example, a named entity recognizer in the natural language processing (NLP) pipeline might classify ranges of text as being the mention of a company. This kind of logic is typically too complicated for the default use of a Lucene or ElasticSearch analyzer on the one hand, and the recognition engine might not be easily integratable into an analyzer. However, it might be desired to store the named entity mentions - e.g. in the form of a special term 'COMP' - in the index just like one would to with synonyms: With the same offset as the original text and with position increment 0. This would cause highlighting to work as expected: When the term 'COMP' is searched for, all the recognized text ranges would be highlighted by the ElasticSearch built-in highlighting features, just like with synonyms.

The accepted format is the exact JSON format used by the SolrPreanalyzedField described here: https://wiki.apache.org/solr/JsonPreAnalyzedParser

After installing the plugin, all you have to do is to define a mapping using this mapper, for example:

     "entityAnnotatedDocumentText": {
          "type": "preanalyzed"
          "analyzer": "my_custom_analyzer",
          "store": "yes",
          "term_vector": "with_positions_offsets"
      }
        
The "analyzer" setting will be used for search query analysis and have no consequences for indexing.
Most, if not all, options for string fields are applicable to the preanalyzed mapping type.

PLEASE NOTE: The author of this software makes no guarantee as to whether the software works as intended, causes no damage to your application environment or that it won't have side effects. While this software has been applied successfully in the author's own work, he is not specifically an ElasticSearch related developer and might do things differently from what the ElasticSearch developers intended. Please test the use of this software in your environment thoroughly before setting up a production system employing this plugin.
