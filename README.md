# spark-miner
Text mining/preprocessing package for Spark. This package is purely a pet project. 

## 1. Keyword extraction
Keyword extraction is tasked with the automatic identification of terms that best describe the subject of a document.

Supported algorithms (implemented or on the roadmap):

1) Rapid Automatic Keyword Extraction (RAKE) as described in Text Mining: Applications and Theory by Michael W. Berry, Jacob Cogan.

2) TextRank as described in [TextRank: Bringing Order into Texts by Rada Mihalcea and Paul Tarau.](https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf)

3) Maui as described in [Human-competitive automated topic indexing by Olena Medelyan](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.178.2104&rep=rep1&type=pdf)

## 2. Embedding
[Word|Phrase] Embedding is a technique for transforming words or phrases from the vocabulary into vectors of real numbers in a low-dimensional space relative to the vocabulary size.

Supported algorithms (implemented or on the roadmap):

1) GloVe as descibed in [Jeffrey Pennington, Richard Socher, and Christopher D. Manning. 2014. GloVe: Global Vectors for Word Representation.](http://nlp.stanford.edu/pubs/glove.pdf)

2) Doc2Vec as described in [Quoc Le, Tomas Mikolov. 2014. Distributed Representations of Sentences and Documents](https://arxiv.org/pdf/1405.4053v2.pdf)
