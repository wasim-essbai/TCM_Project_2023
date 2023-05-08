const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    id_user: String,
    id_user_to_share: String,
    idx_shared_talks: Array
}, { collection: 'tedx_user_shares' });

module.exports = mongoose.model('user_shares', talk_schema);