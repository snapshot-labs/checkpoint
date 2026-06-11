export function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Rejects with a timeout error if the provided promise does not settle within
 * `ms` milliseconds. Used to guard provider network calls that cannot be passed
 * an AbortSignal, so a hung socket becomes a thrown error the retry loops can
 * handle instead of silently freezing an indexer forever.
 */
export function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  label = 'request'
): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out after ${ms}ms`)),
      ms
    );
  });

  return Promise.race([promise, timeout]).finally(() =>
    clearTimeout(timer)
  ) as Promise<T>;
}

export function chunk<T>(array: T[], chunkSize: number) {
  const chunks = [] as T[][];
  let index = 0;

  while (index < array.length) {
    chunks.push(array.slice(index, chunkSize + index));
    index += chunkSize;
  }

  return chunks;
}
