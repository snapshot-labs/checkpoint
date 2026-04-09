import { HypersyncBlockFetcher } from '../../../../../src/providers/evm/fetchers/hypersync';

describe('HypersyncBlockFetcher', () => {
  it('should be instantiated with apiToken and rpcUrl', () => {
    const fetcher = new HypersyncBlockFetcher({
      apiToken: 'test-token',
      rpcUrl: 'https://rpc.example.com'
    });
    expect(fetcher).toBeDefined();
  });
});
