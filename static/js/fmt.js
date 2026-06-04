export function fmt(v, unit) {
  if (v == null || isNaN(v)) return "—";
  const a = Math.abs(v);
  if (unit === "") return v.toFixed(3);
  if (a >= 1000) return v.toFixed(1);
  return v.toFixed(2);
}
