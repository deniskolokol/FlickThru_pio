# FlickThru

FlickThru - prediction.io


##Quick Start

 1. [Install the PredictionIO framework](https://docs.prediction.io/install/) **be sure to choose HBase and Elasticsearch** for storage. This template requires Elasticsearch.
 2. Make sure the PIO console and services are running, check with `pio status`
 
###Import Sample Data

1. Create a new app name, change `appName` in `engine.json`
2. Run `pio app new **your-new-app-name**`
3. Import sample events by running `python work/import_sample.py --access_key **your-access-key**` where the key can be retrieved with `pio app list`
4. The engine.json file in the root directory of your new UR template is set up for the data you just imported (make sure to create a new one for your data) Edit this file and change the `appName` parameter to match what you called the app in step #2
5. Perform `pio build`, `pio train`, and `pio deploy`
6. To execute some sample queries run `./examples/query.sh`

### Source

https://github.com/PredictionIO/template-scala-parallel-universal-recommendation/
 
## References

 * Other documentation of the algorithm is [here](http://mahout.apache.org/users/algorithms/intro-cooccurrence-spark.html)
 * A free ebook, which talks about the general idea: [Practical Machine Learning](https://www.mapr.com/practical-machine-learning).
 * A slide deck, which talks about mixing actions and other correlator types, including content-based ones: [Creating a Unified Recommender](http://www.slideshare.net/pferrel/unified-recommender-39986309?ref=http://occamsmachete.com/ml/)
 * Two blog posts: What's New in Recommenders: part [#1](http://occamsmachete.com/ml/2014/08/11/mahout-on-spark-whats-new-in-recommenders/) [#2](http://occamsmachete.com/ml/2014/09/09/mahout-on-spark-whats-new-in-recommenders-part-2/)
 * A post describing the log-likelihood ratio: [Surprise and Coincidence](http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html) LLR is used to reduce noise in the data while keeping the calculations O(n) complexity.
