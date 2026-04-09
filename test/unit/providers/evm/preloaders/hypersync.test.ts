import { HypersyncPreloader } from '../../../../../src/providers/evm/preloaders/hypersync';

describe('HypersyncPreloader', () => {
  it('should be instantiated with apiToken and rpcUrl', () => {
    const preloader = new HypersyncPreloader({
      apiToken: 'test-token',
      rpcUrl: 'https://rpc.example.com'
    });
    expect(preloader).toBeDefined();
  });
});
