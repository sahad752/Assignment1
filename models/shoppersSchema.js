const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const shoppersSchema = new Schema({
    action:{
        type:String,
    },time_stamp:{
        type:String,
    },campaign_id:{
        type:String,
    },publisher_id:{
        type:String,
    },product_id:{
        type:String,
    },merchant:{
        type:String
    },shopper_id:{
        type:String
    },hashed_ip:{
        type:String
    },user_agent:{
        type:String
    },aff_source:{
        type:String
    }, 
    aff_medium:{
        type:String
    }, 
    aff_term:{
        type:String
    }, 
    aff_campaign:{
        type:String
    }, 
    aff_content:{
        type:String
    }, parent_org:{
        type:String
    }
});

const ShoppersLog = mongoose.model('ShoppersLog',shoppersSchema);
module.exports  = ShoppersLog;