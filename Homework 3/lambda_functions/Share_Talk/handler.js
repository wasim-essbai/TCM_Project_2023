const connect_to_db = require('./db');

// SHARE TALK

const user_shares = require('./UserShares');
const talk = require('./Talk');
const AWS = require('aws-sdk')
const cognito = new AWS.CognitoIdentityServiceProvider()

function add_user_share(username, body, callback) {
  
  const query = {"id_user": username,"id_user_to_share": body.id_user_to_share};
  const update = { $push : { "idx_shared_talks" : body.idx}};
  const options = { upsert: true };
  
  user_shares.updateOne(query, update, options).then(updated => {
                callback(null, {
                    statusCode: 200,
                    body: "UPDATED!" 
                })
            })
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not share the talk. Try again.'
                })
            );
}

module.exports.share_talk = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {};
    if (event.body) {
        body = JSON.parse(event.body);
    }
    
    let idToken = event.headers.Authorization;
    
    if(!idToken) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not share talk. Authnetication required.'
        })
        return;
    }
    
    if(!body.idx) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not share talk. Idx is null.'
        })
        return;
    }
    
    if(!body.id_user_to_share) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not share talk. Id user is null.'
        })
        return;
    }
    
    const username = event.requestContext.authorizer.claims['cognito:username']
    try{
        const params = {
            UserPoolId: process.env.userPoolId,
            Filter: `username = \"${body.id_user_to_share}\"`,
        }
        
        cognito.listUsers(params, function(err, data) {
          if (err){
              callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the user to share.'
                })
              return;
          }
          
          if(!data.Users){
              callback(null, {
                            statusCode: 500,
                            headers: { 'Content-Type': 'text/plain' },
                            body: 'User to share not found.'
                })
            return;
          }
        });
    } catch(err){
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the user to share.'
                })
    }
    
    connect_to_db().then(() => {
        talk.find({_id: body.idx}).then(talkToShare => {
                      console.log('TedxTalk to Share', talkToShare)
                      if (!talkToShare[0]) {
                        callback(null, {
                            statusCode: 500,
                            body: 'Talk to share not found'
                        })
                        
                        return;
                      } 
                      
                      add_user_share(username, body, callback); 
                  })
                  .catch(err =>
                    callback(null, {
                        statusCode: err.statusCode || 500,
                        headers: { 'Content-Type': 'text/plain' },
                        body: 'Could not fetch the talk to share.'
                    })
                );
    });
    
};