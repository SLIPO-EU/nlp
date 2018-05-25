# nlp
Snippets for natural language processing.

## Methods for Semantic Annotation of POIs
- Named Entity Recognition (NER)
- Sentiment Detection

## Implementation
Stanford CoreNLP (v. 3.6.0) wrapper for Apache Spark

com.databricks.spark.corenlp.functions: 
- sentiment: Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong positive)
- ner: Generates the named entitiy tags of the sentence. 5 classes: Location, Person, Organization, Number, Date ("O" means no tag)


## Example
sentence: really|O excited|O to|O hear|O of|O this|O restaurant|O coming|O to|O toronto|LOCATION |O [3] 
sentence: word1|NER-Tag ... wordn|NER-Tag [sentiment]
