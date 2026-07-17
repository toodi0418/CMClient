export interface ValidationIssue {
  path: string;
  code: string;
}

export function requiredString(value: unknown, path: string): ValidationIssue[] {
  return typeof value === "string" && value.trim() !== ""
    ? []
    : [{ path, code: "VALIDATION_REQUIRED_STRING" }];
}

export function boundedInteger(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number
): ValidationIssue[] {
  return Number.isInteger(value) && (value as number) >= minimum && (value as number) <= maximum
    ? []
    : [{ path, code: "VALIDATION_BOUNDED_INTEGER" }];
}
