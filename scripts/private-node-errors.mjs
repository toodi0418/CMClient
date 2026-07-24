export class PrivateNodeRuntimeError extends Error {
  constructor(code) {
    super(code);
    this.name = "PrivateNodeRuntimeError";
    this.code = code;
  }
}

export function failPrivateNode(code) {
  throw new PrivateNodeRuntimeError(code);
}
