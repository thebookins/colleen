// TODO: add logic for dropped update (i.e. during dyno restart)
var cron = require('node-cron');

const Queue = require('bull');
const REDIS_URL = process.env.REDIS_URL;
const workQueue = new Queue('work', REDIS_URL);

cron.schedule('* * * * *', async () => {
  console.log('running a task every minute');
  const command = {
    type: 'update',
    args: [Date.now().toString()]
  }
  let job = await workQueue.add(command);

  // TODO: maybe set up three queues and do something like this?
  // await pumpQueue.add({'step'})
  // await cgmQueue.add({'step'})
  // await personQueue.add({'step'})
});