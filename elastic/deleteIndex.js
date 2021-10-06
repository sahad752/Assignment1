//deleteIndex.js
const elasticsearch = require('elasticsearch');

const deleteIndex = async (indexName) => {
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
     await client.indices.delete({index: indexName});
     console.log('All index is deleted');
     return "All index is deleted"
     } catch (e) {
               if (e.status === 404) {
                  console.log('Index Not Found');
                  return "index not found";
               } else {
                   throw e;
               }
     }
}
// deleteIndex();
module.exports = deleteIndex

