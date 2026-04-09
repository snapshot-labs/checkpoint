import { CheckpointConfig } from '../../../types';
import { Logger } from '../../../utils/logger';
import { BlockFetcher, FetchedBlock, Preloader } from './types';
import { RpcBlockFetcher } from './rpc';
import { HypersyncPreloader } from './hypersync';

export { BlockFetcher, FetchedBlock, Preloader, RpcBlockFetcher, HypersyncPreloader };

export function createFetchers(
  config: CheckpointConfig,
  log: Logger
): { fetcher: BlockFetcher; preloader?: Preloader } {
  const fetcher = new RpcBlockFetcher(config.network_node_url);

  if (config.hypersync_api_token) {
    log.info('using HyperSync preloader');

    const preloader = new HypersyncPreloader({
      apiToken: config.hypersync_api_token,
      rpcUrl: config.network_node_url
    });

    return { fetcher, preloader };
  }

  return { fetcher };
}
