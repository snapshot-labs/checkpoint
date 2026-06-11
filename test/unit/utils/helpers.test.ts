import { chunk, withTimeout } from '../../../src/utils/helpers';

describe('withTimeout', () => {
  it('resolves when the promise settles before the timeout', async () => {
    await expect(withTimeout(Promise.resolve('ok'), 50)).resolves.toBe('ok');
  });

  it('rejects when the promise never settles within the timeout', async () => {
    const neverResolves = new Promise(() => {});

    await expect(withTimeout(neverResolves, 20, 'getLogs')).rejects.toThrow(
      'getLogs timed out after 20ms'
    );
  });
});

describe('chunk', () => {
  it('should chunk array', () => {
    const array = [1, 2, 3, 4, 5, 6, 7, 8];
    const chunked = chunk(array, 3);

    expect(chunked).toEqual([
      [1, 2, 3],
      [4, 5, 6],
      [7, 8]
    ]);
  });
});
