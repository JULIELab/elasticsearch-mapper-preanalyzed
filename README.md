elasticsearch-mapper-preanalyzed
================================

An ElasticSearch mapper plugin that allows to index preanalyzed TokenStreams, i.e. to circumvent an index analyzer in ElasticSearch and instead specifying each token to be indexed separatly.

This can be useful if a stand-alone text analysis engine is available, for instance a UIMA or GATE pipeline, and you would like to transfer the analysis results one to one into the ElasticSearch index.
For example, a named entity recognizer in the natural language processing (NLP) pipeline might classify ranges of text as being the mention of a company. This kind of logic is typically too complicated for the default use of a Lucene or ElasticSearch analyzer on the one hand, and the recognition engine might not be easily integratable into an analyzer. However, it might be desired to store the named entity mentions - e.g. in the form of a special term 'COMP' - in the index just like one would to with synonyms: With the same offset as the original text and with position increment 0. This would cause highlighting to work as expected: When the term 'COMP' is searched for, all the recognized text ranges would be highlighted by the ElasticSearch built-in highlighting features, just like with synonyms.

The accepted format is the exact JSON format used by the SolrPreanalyzedField described here: https://wiki.apache.org/solr/JsonPreAnalyzedParser

After installing the plugin, all you have to do is to define a mapping using this mapper, for example:

     "entityAnnotatedDocumentText": {
          "type": "preanalyzed"
          "search_analyzer": "my_custom_analyzer",
          "store": "yes",
          "term_vector": "with_positions_offsets"
      }
        
Most, if not all, options for string fields are applicable to the preanalyzed mapping type.

PLEASE NOTE: The author of this software makes no guarantee as to whether the software works as intended, causes no damage to your application environment or that it won't have side effects. While this software has been applied successfully in the author's own work, he is not specifically an ElasticSearch related developer and might do things differently from what the ElasticSearch developers intended. Please test the use of this software in your environment thoroughly before setting up a production system employing this plugin.
