const throng = require('throng');
const Queue = require('bull');
const MongoClient = require('mongodb').MongoClient;
const Helper = require('./helper');

const REDIS_URL = process.env.REDIS_URL;

const connectToDB = () => {
  return MongoClient.connect(process.env.MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    })
    .then(client => {
      return client.db();
    });
};


// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 2;

// The maxium number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
let maxJobsPerWorker = 50;

function start() {
  connectToDB().then(db => {
    helper = Helper(db);

    // NOTE: this only needs be done once, and can be done on the server
    // documents expire after two hours
    db.collection('cgm-events').createIndex( { "readDate": 1 }, { expireAfterSeconds: 60 * 60 * 2 } );

    // NOTE: this only needs be done once, and can be done on the server
    // expires after 24 hours
    db.collection('meals').createIndex( { "date": 1 }, { expireAfterSeconds: 60 * 60 * 24 } );

    let workQueue = new Queue('work', REDIS_URL);

    workQueue.process(maxJobsPerWorker, async (job) => {
      console.log(`worker got data ${JSON.stringify(job.data)}`);
    //   if (job.data.type !== 'update') return;
    //   await helper.pump.step();
      await helper.person.step();
      await helper.cgm.step();
    });
  });
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
