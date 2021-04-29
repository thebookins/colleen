const basalRate = 0; // U/hr
const dt = 1; // min

const pump = (state, action, ...args) => {
  // TODO: maybe change the name to step?
  const basal = (receiver) => {
    state.clock += 1;
    return bolus(dt * basalRate / 60, receiver);
  }

  const bolus = (units, receiver) => {
    state.reservoir -= units;
    receiver(units);
    return state;
  }

  fn = (() => {
    switch(action) {
      case 'basal': return basal;
      case 'bolus': return bolus;
      default: return () => {};
    }
  })();

  return (fn.length <= args.length) ?
    fn(...args) :
    (...more) => pump(state, action, ...args, ...more);
};

module.exports = pump;
