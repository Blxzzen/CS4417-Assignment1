// aggregations.js

// 2.1) [10pts] Produce a list of users, together with the total number of times they tweeted, sorted in decreasing order.
db.tweets.aggregate([
    {
        $group: {
            _id: "$user.screen_name",
            tweetCount: { $sum: 1 },
        },
    },
    { $sort: { tweetCount: -1 } },
]);

// 2.2) [10pts] Produce a list of place names, together with the total number of tweets from that place name, sorted in decreasing order.
db.tweets.aggregate([
    { $match: { place: { $ne: null } } },
    {
        $group: {
            _id: "$place.full_name",
            tweetCount: { $sum: 1 },
        },
    },
    { $sort: { tweetCount: -1 } },
]);

// 2.3) [15pts] Produce a list of users, together with the total number of replies to that user, sorted in decreasing order.
db.tweets.aggregate([
    { $match: { in_reply_to_screen_name: { $ne: null } } },
    {
        $group: {
            _id: "$in_reply_to_screen_name",
            replyCount: { $sum: 1 },
        },
    },
    { $sort: { replyCount: -1 } },
]);

// 2.4) [15pts] Produce a list of users, together with the total number of hashtags used by that user, sorted in decreasing order.
db.tweets.aggregate([
    {
        $project: {
            user: "$user.screen_name",
            hashtagCount: {
                $size: {
                    $ifNull: ["$entities.hashtags", []],
                },
            },
        },
    },
    {
        $group: {
            _id: "$user",
            hashtagCount: { $sum: "$hashtagCount" },
        },
    },
    { $sort: { hashtagCount: -1 } },
]);