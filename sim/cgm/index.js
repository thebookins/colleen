var apn = require('apn');

module.exports = () => {
  const options = {
    token: {
      key: __dirname + "/AuthKey_23NRN4PHVP.p8",
      keyId: "23NRN4PHVP",
      teamId: "8ZWMLSD6JG"
    },
    production: false
  };

  const apnProvider = new apn.Provider(options);

  const postAPN = () => {
    let deviceToken = "73279d1446f62f51cd12f090dc2a551ec1f127c28869bc0096902b7300bacbdb"

    var note = new apn.Notification();
    note.contentAvailable = 1;
    // NOTE: probably not necessary to set this field...
    note.sound = "";
    note.topic = "com.8ZWMLSD6JG.loopkit.Loop"

    apnProvider.send(note, deviceToken).then( (result) => {
      console.log(JSON.stringify(result));
      // see documentation for an explanation of result
    });
  };

  return { postAPN };
}
