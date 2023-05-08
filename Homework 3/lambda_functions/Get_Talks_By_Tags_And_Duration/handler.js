const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

function compareTalkPopularity(talk1, talk2){
    const like1String = talk1.num_likes;
    const like2String = talk2.num_likes;
    
    const num_views1 = parseInt(talk1.num_views.replaceAll(",", ""))
    const num_views2 = parseInt(talk2.num_views.replaceAll(",", ""))
    
    if(!like1String & !like2String){
        return 0;
    }
    
    if(!like1String){
        return -1;
    }
    
    if(!like2String){
        return 1;
    }
    
    let num_likes1 = parseInt(like1String.substring(0, like1String.length - 1));
    switch (like1String[like1String.length - 1]) {
        case 'K':
            num_likes1 = num_likes1 * 1000;
            break;
        case 'M':
            num_likes1 = num_likes1 * 1000 * 1000;
            break;
        case 'B':
            num_likes1 = num_likes1 * 1000 * 1000 * 1000;
            break;
    }
    let num_likes2 = parseInt(like2String.substring(0, like2String.length - 1));
    
    switch (like2String[like2String.length - 1]) {
        case 'K':
            num_likes2 = num_likes2 * 1000;
            break;
        case 'M':
            num_likes2 = num_likes2 * 1000 * 1000;
            break;
        case 'B':
            num_likes2 = num_likes2 * 1000 * 1000 * 1000;
            break;
    }
    
    const popularity_score1 = num_likes1 / num_views1;
    const popularity_score2 = num_likes2 / num_views2;
    
    return -(popularity_score1 - popularity_score2); 
}

module.exports.get_by_tags_and_duration = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    
    let agg_query = [];
    let find_query = {};
    
    if(body.tags) {
       find_query.tags = { $in: body.tags};
    }
    
    if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    if (!body.page) {
        body.page = 1
    }
    
    
   let condition_1 = {
        $add:[
                {$multiply: [{$toInt: {$substr: ["$duration", 0, 1]}}, 3600]},
                {$multiply: [{$toInt: {$substr: ["$duration", 3, 2]}}, 60]}
            ]
    }
    let condition_2 = {
        $add:[
                {$multiply: [{$toInt: {$substr: ["$duration", 0, 2]}}, 60]},
                {$multiply: [{$toInt: {$substr: ["$duration", 3, 2]}}, 1]}
            ]
    }
    let condition_3 = {
        $add:[
                {$multiply: [{$toInt: {$substr: ["$duration", 0, 1]}}, 60]},
                {$multiply: [{$toInt: {$substr: ["$duration", 2, 2]}}, 1]}
            ]
    }
    
    let addTotDuration = {
        $addFields:
        {
           "total_duration" : 
           {
               $switch: {
                  branches: [
                     { case: { $eq: [{ $strLenCP: "$duration" }, 6] }, then: condition_1 },
                     { case: { $eq: [{ $strLenCP: "$duration" }, 5] }, then: condition_2 },
                     { case: { $eq: [{ $strLenCP: "$duration" }, 4] }, then: condition_3 }
                  ]
               }
           }
        }
    };

    if (body.duration) {
        agg_query.push(addTotDuration);
        find_query.total_duration = {$lte: body.duration};
    }
    
    agg_query.push({$match: find_query});
    agg_query.push({$skip : (body.doc_per_page * body.page) - body.doc_per_page});
    agg_query.push({$limit : body.doc_per_page});
    
    
    
    connect_to_db().then(() => {
        console.log("Aggregate query", agg_query);
        talk.aggregate(agg_query)
        .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks.sort(compareTalkPopularity))
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};