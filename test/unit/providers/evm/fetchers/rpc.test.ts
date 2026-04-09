import { RpcBlockFetcher } from '../../../../../src/providers/evm/fetchers/rpc';

describe('RpcBlockFetcher', () => {
  it('should be instantiated with a network URL', () => {
    const fetcher = new RpcBlockFetcher('https://rpc.example.com');
    expect(fetcher).toBeDefined();
  });
});
