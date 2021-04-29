const events = require('events');
var apn = require('apn');

module.exports = (db) => {
  const eventEmitter = new events.EventEmitter();
  const everyFiveMinutes = 5 * 60 * 1000;

  const options = {
    token: {
      key: __dirname + "/AuthKey_23NRN4PHVP.p8",
      keyId: "23NRN4PHVP",
      teamId: "8ZWMLSD6JG"
    },
    production: false
  };

  db.createCollection('cgm', { capped : true, size: 8192, max : 36 } )
  // TODO: add error handler here

  const apnProvider = new apn.Provider(options);

  const latestGlucose = {
    readDate: new Date(0),
    glucose: null
  };

  let read = () => 0;

  setInterval(() => {
    // TODO: lose the local variable here: use the db instead
    latestGlucose.readDate = new Date(),
    latestGlucose.glucose = read();
    const latestGlucose2 = {
      readDate: latestGlucose.readDate,
      glucose: latestGlucose.glucose
    }

    db.collection('cgm').insertOne(latestGlucose2, function(err, doc) {
      if (err) {
        // handleError(res, err.message, "Failed to create new expense.");
        console.log(`Failed to insert glucose: ${err}.`);
      }
    });

    let deviceToken = "c31ce3c0585db5744839accc7c6a6d42eb7649d0b9608b8df1757be077947240"

    var note = new apn.Notification();
    note.contentAvailable = 1;
    // NOTE: probably not necessary to set this field...
    // note.sound = "";
    note.topic = "com.8ZWMLSD6JG.loopkit.Loop"

    apnProvider.send(note, deviceToken).then( (result) => {
      console.log(JSON.stringify(result));
      // see documentation for an explanation of result
    });

//    eventEmitter.emit('glucose', read());
}, everyFiveMinutes);

  const api = {
    on: (message, callback) => eventEmitter.on(message, callback),
    set read(fn) {
      read = fn;
    },
    get glucose() {
      return latestGlucose;
    }
  };

  return api;
}

// NOTE: the old one
//
// module.exports = (t1d) => {
//   const eventEmitter = new events.EventEmitter();
//   const history = []
//
//   setInterval(() => {
//     const glucose = t1d.sense();
//     history.unshift({
//       readDate: new Date(),
//       glucose
//     });
//     if (history.length > 36) {
//       history.pop();
//     }
//     eventEmitter.emit('glucose', glucose);
//   }, 5 * 60 * 1000); // every 5 minutes
//
//   return {
//     on: (message, callback) => eventEmitter.on(message, callback),
//     get history() {
//       return history;
//     },
//   };
// };
