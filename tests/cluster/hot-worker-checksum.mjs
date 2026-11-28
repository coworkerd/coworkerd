export function checksumFor(iterations) {
  return function checksum() {
    let value = 1234;
    for (let i = 0; i < iterations; ++i) value = (Math.imul(value ^ i, 1664525) + 1013904223) | 0;
    return value;
  };
}
