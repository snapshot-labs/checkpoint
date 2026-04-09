import { EvmProvider } from '../../../../src/providers/evm/provider';
import { FetchedBlock } from '../../../../src/providers/evm/preloaders/types';

describe('EvmProvider block cache', () => {
  it('should store and retrieve cached blocks', () => {
    const provider = Object.create(EvmProvider.prototype);
    provider.blockCache = new Map<number, FetchedBlock>();

    const block100: FetchedBlock = {
      number: 100,
      hash: '0xabc',
      parentHash: '0xdef',
      timestamp: 1700000000
    };
    const block101: FetchedBlock = {
      number: 101,
      hash: '0x123',
      parentHash: '0xabc',
      timestamp: 1700000012
    };

    provider.blockCache.set(100, block100);
    provider.blockCache.set(101, block101);

    expect(provider.blockCache.get(100)).toEqual(block100);
    expect(provider.blockCache.get(101)).toEqual(block101);
    expect(provider.blockCache.has(102)).toBe(false);
  });
});
