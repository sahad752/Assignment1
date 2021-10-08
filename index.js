const express = require('express');
const shoppersSchema = require('./models/shoppersSchema')
require('dotenv').config()
const mongoose = require('mongoose');
var elasticclient = require('./elastic/connection');  
const start = require('./elastic/LoadDataToEls');
const deleteIndex = require('./elastic/deleteIndex');
const csvtojson = require('csvtojson');


const fileName = "shopper_actions3.csv";
var url = "mongodb+srv://sahad:sahad@cluster0.i3c1r.mongodb.net/CustomersLogDb?retryWrites=true&w=majority";
mongoose.connect(url,{useNewurlParser:true,useUnifiedTopology:true}).then((result)=>console.log("connected to db"));

const app = express();
const PORT = process.env.PORT || 5000;
app.use(express.json());
app.listen(PORT, () => console.log(`Server listening in port ${PORT}`))



app.get('/',(req,res)=>{
    res.send('Welcome to my Shopalyst Api  ');
});



//To check status of elastic search
app.get('/CheckEdbHealth',(req,res)=>{
    elasticclient.ping({
        requestTimeout: 30000,
    }, function(error) {
        if (error) {
            res.send(404)
            console.error('elasticsearch cluster is down!');
        } else {
            res.send("Everything ok")
            console.log('Everything is ok');
        }
    });
})



//add single data only to Els
app.post('/addtoEdb',(req,res)=>{
    if(req.body.indexName){
        elasticclient.index({
            index: req.body.indexName,
            // id: req.body.id,
            // type: 'posts',
            body: {
                "time_stamp": req.body.time_stamp,
                "action":req.body.action,
                "campaign_id":req.body.campaign_id,
                "publisher_id":req.body.publisher_id,
                "product_id":req.body.product_id,
                "shopper_id":req.body.shopper_id ,
                "hashed_ip":req.body.hashed_ip,
                "user_agent":req.body.user_agent,
                "aff_source":req.body.aff_source,
                "aff_medium":req.body.aff_medium,
                "aff_term":req.body.aff_term,
                "aff_campaign":req.body.aff_campaign,
                "aff_content":req.body.aff_content,
                "parent_org":req.body.parent_org
            }
        }, function(err, resp, status) {
            if(err){
                res.send("Something went wrong,please checkk els connection ")
            }else{
                res.send(resp)

            }
        });
    }else{
        res.status(400).send("IndexName is required")
    }
})



//main function to get required data 
//on Elastic Search 
app.get('/searchbySession',(req,res)=>{
    if(req.body.brand&&req.body.date_fro&&req.body.date_to){
        elasticclient.search({
            index: 'users_session_logs',
            body: {
                "size": 0, 
                "query": {
                      "bool": {
                          "must": [
                            {
                      "range": {
                      "time_stamp": {
                      "format": "strict_date_optional_time", 
                      "gte": req.body.date_fro,
                      "lt": req.body.date_to
                            }
                           }
                            },
                              {
                                  "match": {
                                      "action": "SESSION_INIT"
                                  }
                              },
                              {
                                  "match": {
                                      "parent_org": req.body.brand
                                  }
                              }
                          ]
                      }
                  }
                ,
                "aggs": {
                      "uniqueShoppersID": {
                        "terms": {
                          "field": "shopper_id.keyword",
                         "size": 10000
                        }
                        
                      }
              }
              }
        }).then(function(resp) {
            // console.log(resp)
            res.send(resp)
            // res.send(resp.hits['hits'])
            // res.status(200).send(resp.aggregations)
            // res.status(200).send(resp.aggregations['uniqueShoppersID']['buckets'])
        }, function(err) {
            res.send(err)
            console.trace(err.message);
        });
    }else{
        res.status(400).send("one of parameters is missing")
    }
    
})





// post single schema to mongo and els
app.post('/addShopperlog', (req,res)=>{

    if(req.body.indexName){
        const post = new shoppersSchema({
            time_stamp: req.body.time_stamp,
            action:req.body.action,
            campaign_id:req.body.campaign_id,
            publisher_id:req.body.publisher_id,
            product_id:req.body.product_id,
            shopper_id:req.body.shopper_id ,
            hashed_ip:req.body.hashed_ip,
            user_agent:req.body.user_agent,
            aff_source:req.body.aff_source,
            aff_medium:req.body.aff_medium,
            aff_term:req.body.aff_term,
            aff_campaign:req.body.aff_campaign,
            aff_content:req.body.aff_content,
            parent_org:req.body.parent_org
        });
        
        try{
            post.save().then(()=>{  
                // res.send(post);
            })
    
            elasticclient.index({
                index: req.body.indexName,
                body: {
                    "time_stamp": req.body.time_stamp,
                    "action":req.body.action,
                    "campaign_id":req.body.campaign_id,
                    "publisher_id":req.body.publisher_id,
                    "product_id":req.body.product_id,
                    "shopper_id":req.body.shopper_id ,
                    "hashed_ip":req.body.hashed_ip,
                    "user_agent":req.body.user_agent,
                    "aff_source":req.body.aff_source,
                    "aff_medium":req.body.aff_medium,
                    "aff_term":req.body.aff_term,
                    "aff_campaign":req.body.aff_campaign,
                    "aff_content":req.body.aff_content,
                    "parent_org":req.body.parent_org
                }
            }, function(err, resp, status) {
                if(err){
                    console.log("Something went wrong")
                }else{
                    res.send(resp)
                    // console.log(resp)
    
                }
            });
            
        }catch(err){
            res.status(500).send(err);
        }
    }else{
        res.status(400).send("Index Name is required")
    }
   
});








//aggregation on mongo by giving any action
app.get("/getbySession",async (req,res)=>{
    const from_time = req.body.from_time
    const to_time =req.body.to_time;

    shoppersSchema.aggregate(
        [
            { $match: { time_stamp: { $gt: from_time, $lt: to_time } } },
            {
                $match: {
                       action: req.body.action
                }
             },
             {
                $match: {
                    parent_org: req.body.brand
                }
             },
              {$group : {_id : "$shopper_id", totalNumber : {$sum : 1}}}
           
          
         ],
       ).then(function(docs){
        try {
            console.log(docs)
            res.send(docs);
         }catch{
        response.status(404).send(error);
         }      
     })

    console.log(from_time+" to "+to_time)
});


//main function to get required data
//unique users aggregation on mongo
app.get("/getuniqueinitUsers",async (req,res)=>{
    const from_time = req.body.from_time
    const to_time =req.body.to_time;

    shoppersSchema.aggregate(
        [
            { $match: { time_stamp: { $gt: from_time, $lt: to_time } } },
            {
                $match: {
                       action: "SESSION_INIT"
                }
             },
             {
                $match: {
                    parent_org: req.body.brand
                }
             },
             {$group : {_id : "$shopper_id", totalNumber : {$sum : 1}}}

          
         ],
       ).then(function(docs){
        try {
            console.log(docs)
            res.send(docs);
         }catch{
        response.status(404).send(error);
         }      
     })

    console.log(from_time+" to "+to_time)
});

//on mongo delete
app.get("/delete", async (request, response) => {
    const blogs = await shoppersSchema.deleteMany({});
    try {
      response.send(" deleted All records");
    } catch (error) {
      response.status(500).send(error);
    }
});


///load data to db on mongo
app.post('/loadToM',async(req,res)=>{
    csvtojson().fromFile(fileName).then(source => {
        console.log(source.length);
        var logs = []
        for (var i = 0; i < source.length; i++) {
            var action = source[i]["action"],
            time_stamp = source[i]["time_stamp"],
            campaign_id = source[i]["campaign_id"],
            publisher_id = source[i]["publisher_id"],
            product_id = source[i]["product_id"],
            shopper_id = source[i]["shopper_id"],
            hashed_ip = source[i]["hashed_ip"],
            user_agent = source[i]["user_agent"],
            aff_source = source[i]["aff_source"],
            aff_medium = source[i]["aff_medium"],
            aff_term = source[i]["aff_term"],
            aff_campaign = source[i]["aff_campaign"],
            aff_content = source[i]["aff_content"],
            parent_org = source[i]["parent_org"]
    
            const post = new shoppersSchema({
                time_stamp:time_stamp,
                action:action,
                campaign_id:campaign_id,
                publisher_id:publisher_id,
                product_id:product_id,
                shopper_id:shopper_id ,
                hashed_ip:hashed_ip,
                user_agent:user_agent,
                aff_source:aff_source,
                aff_medium:aff_medium,
                aff_term:aff_term,
                aff_campaign:aff_campaign,
                aff_content:aff_content,
                parent_org:parent_org
            });

            logs.push(post) 
        }


        shoppersSchema.insertMany(logs).
        then(function(docs){
            console.log("successfully pushed all items");
            res.send(
                "All items stored into database successfully "+logs.length +logs[1]);
        }).
        catch(function(err){
            res.err(500);
        });

    });
});

//add all to elastic 
app.post('/loadCsvToEdb',async(req,res)=>
{
    if(req.body.indexName){
        start(req.body.indexName).then(function(result){
            res.status(200).send(result)
        })
    }else{
        res.status(400).send("indexName is required")
    }
}
);


//delete all from Els
app.get("/deleteOnEls", async (request, response) => {
    if(request.body.indexName){
        deleteIndex(request.body.indexName).then(function(result){
            return response.status( 200 ).send(result);
        })
    }else{
        console.log("IndexName is required")
        response.status(400).send("IndexName is required")
    }

});


//get a single data by id
app.get('/getbyId',(req,res)=>{
    elasticclient.search({
        index: 'users_session_logs3',
        body:{
            "query": {
                "terms": {
                  "_id": [ "O4xnT3wBEeRavhDl90vG"] 
                }
              }
        }
    }).then(function(resp) {
        res.send(resp)
    }, function(err) {
        res.send(err)
        console.trace(err.message);
    });
})



// app.post('/upload', async function(req, res) {

//   var file = JSON.parse(JSON.stringify(req.files))
//   var file_name = file.file.name
//   //if you want just the buffer format you can use it
//   var buffer = new Buffer.from(file.file.data.data)
//   //uncomment await if you want to do stuff after the file is created
//   /*await*/

//   fs.writeFile(file_name, buffer, async(err) => {
//     console.log("Successfully Written to File.");
//     // do what you want with the file it is in (__dirname + "/" + file_name)
//     console.log("end  :  " + new Date())
//     console.log(result_stt + "")
//     fs.unlink(__dirname + "/" + file_name, () => {})
//     res.send(result_stt)
//   });

// });

///load data to db on mongo
app.post('/load',async(req,res)=>{

    if(req.body.indexName){
        csvtojson().fromFile(fileName).then(source => {
            console.log(source.length);
            var logs = []
            for (var i = 0; i < source.length; i++) {
                var action = source[i]["action"],
                time_stamp = source[i]["time_stamp"],
                campaign_id = source[i]["campaign_id"],
                publisher_id = source[i]["publisher_id"],
                product_id = source[i]["product_id"],
                shopper_id = source[i]["shopper_id"],
                hashed_ip = source[i]["hashed_ip"],
                user_agent = source[i]["user_agent"],
                aff_source = source[i]["aff_source"],
                aff_medium = source[i]["aff_medium"],
                aff_term = source[i]["aff_term"],
                aff_campaign = source[i]["aff_campaign"],
                aff_content = source[i]["aff_content"],
                parent_org = source[i]["parent_org"]
        
                const post = new shoppersSchema({
                    time_stamp:time_stamp,
                    action:action,
                    campaign_id:campaign_id,
                    publisher_id:publisher_id,
                    product_id:product_id,
                    shopper_id:shopper_id ,
                    hashed_ip:hashed_ip,
                    user_agent:user_agent,
                    aff_source:aff_source,
                    aff_medium:aff_medium,
                    aff_term:aff_term,
                    aff_campaign:aff_campaign,
                    aff_content:aff_content,
                    parent_org:parent_org
                });
    
                logs.push(post) 
            }
    
    
            shoppersSchema.insertMany(logs).
            then(function(docs){
                console.log("successfully pushed all items");
                res.send(
                    "All items stored into database successfully "+logs.length +logs[1]);
            }).
            catch(function(err){
                res.err(500);
            });
    
        });

        start(req.body.indexName).then(function(result){
            console.log("Inserted successfully to ELS")
        })

    }else{
        res.status(400).send("indexName is required")
    }

  
});
