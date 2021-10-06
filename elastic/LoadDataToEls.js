const _ = require('highland');
const fs = require('fs');
const csv = require('csv-parser');
const elasticsearch = require('elasticsearch');


const start = async (indexName) => {
  const client = new elasticsearch.Client({
     host: 'localhost:9200',
     // log: 'trace',
  });
  await client.ping({
    requestTimeout: 3000
  }, function (error) {
    if (error) {
      console.trace('elasticsearch cluster is down!');
    } else {
      console.log('Elastic search is running.');
    }
  });
  try {
    await client.indices.create({index: indexName});
    console.log('created index');
   } catch (e) {
      if (e.status === 400) {
        console.log('index already exists');
        return "index already exists"
      } else {
        throw e;
      }
    }

    // process file
    let currentIndex = 0;
    const stream = _(
      fs.createReadStream('./shopper_actions3.csv').pipe(
        csv()
      )
    )
.map(data => ({
      ...data,
    //   alternative_names: data.alternative_names.split(''),
      // lon_num: parseFloat(data.lon),
      // lat_num: parseFloat(data.lat),
      // place_rank_num: parseInt(data.place_rank, 10),
      // importance_num: parseFloat(data.importance),

    }))
.map(data => [{
  index: {_index: indexName, _type: 'logs'},
  },
  data,
])
.batch(100)
.each(async entries => {
      stream.pause();
    const body = entries.reduce((acc, val) => acc.concat(val),[]);
    await client.bulk({body});
    currentIndex += 100;
    console.log('Created index :', currentIndex);
    stream.resume();
})
.on('end', () => {
    console.log('done');
    return "All items saved to Database"    
   });
};
// start();
module.exports = start