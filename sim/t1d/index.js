const PWD = require('marjorie').PWD;
// const events = require('events');

const curry = (state, action, ...args) => {
  const model = PWD({ state });

  const step = (U_I) => {
    model.step(U_I);
    return model.state;
  };

  const bolus = (u) => {
    model.bolus(u);
    return model.state;
  };

  // TODO: consider passing a callback for glucose,
  // so that we can maintain the interface and return
  // the model state
  const glucose = () => {
    return model.glucose;
  }

  const fn = (() => {
    switch(action) {
      case 'step': return step;
      case 'bolus': return bolus;
      case 'glucose': return glucose;
      default: return () => state;
    }
  })();

  return (fn.length <= args.length) ?
    fn(...args) :
    (...more) => curry(state, action, ...args, ...more);
};

module.exports = curry;
