import { describe, expect, it } from 'bun:test';
import { chunk } from '../../../src/utils/helpers';

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
