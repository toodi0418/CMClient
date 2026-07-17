export interface Clock {
  now(): Date;
}

export function fixedClock(isoTimestamp: string): Clock {
  const instant = new Date(isoTimestamp);
  if (Number.isNaN(instant.getTime())) {
    throw new Error("TEST_CLOCK_INVALID_TIMESTAMP");
  }

  return {
    now: () => new Date(instant),
  };
}
