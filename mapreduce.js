// mapreduce.js

// 3.1) [40pts] Produce a new collection that contains each hashtag used in the collection of tweets, along with the number of times that hashtag was used.
function myMapper() {
    if (!this.entities || !this.entities.hashtags) return;

    for (const h of this.entities.hashtags) {
        if (!h) continue;

        const tag = (h.text || "").toLowerCase();
        if (tag.length === 0) continue;

        emit(tag, 1);
    }
}

function myReducer(key, values) {
    return Array.sum(values);
}

// writes results into a new collection
db.tweets.mapReduce(myMapper, myReducer, {out: "hashtag_counts",});

// view most used hashtags
db.hashtag_counts.aggregate([{ $sort: { value: -1 } }, { $limit: 50 }]);