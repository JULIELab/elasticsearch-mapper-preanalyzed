elasticsearch-mapper-preanalyzed
================================

An ElasticSearch mapper plugin that allows to index preanalyzed TokenStreams, i.e. to circumvent an index analyzer in ElasticSearch and instead specifying each token to be indexed separatly.

The accepted format is the exact JSON format used by the SolrPreanalyzedField described here: https://wiki.apache.org/solr/JsonPreAnalyzedParser
