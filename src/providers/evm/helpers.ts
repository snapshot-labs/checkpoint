import { CustomJsonRpcError } from './types';

type Range = {
  from: number;
  to: number;
};

export function getRangeHint(err: unknown, currentRange: Range): Range | null {
  if (!(err instanceof CustomJsonRpcError)) {
    return null;
  }

  // Infura (code: -32005)
  if (err.code === -32005 && err.data.from && err.data.to) {
    const from = parseInt(err.data.from, 16);
    const to = parseInt(err.data.to, 16);

    if (isFinite(from) && isFinite(to)) {
      return {
        from,
        to
      };
    }

    return null;
  }

  // Ankr (code: -32062): Block range is too large
  // Edge RPC (code: -32012): getLogs request exceeded max allowed range
  if (err.code === -32062 || err.code === -32012) {
    // We have no range in the error data, so we return the current range, but half as long
    const to =
      currentRange.from + Math.ceil((currentRange.to - currentRange.from) / 2);

    // Range this small can't be shrunk further, retrying it would loop forever
    if (to >= currentRange.to) {
      return null;
    }

    return {
      from: currentRange.from,
      to
    };
  }

  return null;
}
