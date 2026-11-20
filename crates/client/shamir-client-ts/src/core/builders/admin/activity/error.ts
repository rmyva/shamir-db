export class ActivityProtocolError extends Error {
  readonly retryable = false;

  constructor(readonly code: 'shape_mismatch' | 'unexpected_response', message: string) {
    super(message);
    this.name = 'ActivityProtocolError';
  }
}
