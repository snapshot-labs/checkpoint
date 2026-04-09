import { EvmProvider } from '../../../../src/providers/evm/provider';

describe('EvmProvider block timestamp cache', () => {
  it('should store and retrieve cached timestamps', () => {
    const provider = Object.create(EvmProvider.prototype);
    provider.blockTimestampCache = new Map<number, number>();

    provider.cacheBlockTimestamp(100, 1700000000);
    provider.cacheBlockTimestamp(101, 1700000012);

    expect(provider.blockTimestampCache.get(100)).toBe(1700000000);
    expect(provider.blockTimestampCache.get(101)).toBe(1700000012);
    expect(provider.blockTimestampCache.has(102)).toBe(false);
  });
});
