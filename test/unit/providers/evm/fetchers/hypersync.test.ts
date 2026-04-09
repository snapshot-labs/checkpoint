import { HypersyncBlockFetcher } from '../../../../../src/providers/evm/fetchers/hypersync';

jest.mock('@envio-dev/hypersync-client', () => ({
  HypersyncClient: jest.fn().mockImplementation(() => ({
    getHeight: jest.fn(),
    getChainId: jest.fn(),
    collect: jest.fn()
  }))
}));

describe('HypersyncBlockFetcher', () => {
  it('should be instantiated with chainId, apiToken, and rpcUrl', () => {
    const fetcher = new HypersyncBlockFetcher({
      chainId: 1,
      apiToken: 'test-token',
      rpcUrl: 'https://rpc.example.com'
    });
    expect(fetcher).toBeDefined();
  });
});
