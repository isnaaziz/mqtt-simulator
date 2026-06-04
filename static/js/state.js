export const tags = {};
export const breakers = {};

export function computeEnergize() {
  const cl = (n) => breakers[n] && breakers[n].state === "closed";
  const inc = cl("CB_INC");
  const ups = inc && cl("CB_UPS");
  const load = cl("CB_LOAD") && (ups || (inc && cl("CB_BYPASS")));
  return { grid: true, inc, ups, load };
}
