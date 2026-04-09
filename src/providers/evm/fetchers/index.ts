import { CheckpointConfig } from '../../../types';
import { Logger } from '../../../utils/logger';
import { BlockFetcher, FetchedBlock } from './types';
import { RpcBlockFetcher } from './rpc';
import { HypersyncBlockFetcher } from './hypersync';

export { BlockFetcher, FetchedBlock, RpcBlockFetcher, HypersyncBlockFetcher };

export function createBlockFetcher(
  config: CheckpointConfig,
  log: Logger
): BlockFetcher {
  if (config.hypersync_api_token) {
    log.info('using HyperSync block fetcher');

    return new HypersyncBlockFetcher({
      apiToken: config.hypersync_api_token,
      rpcUrl: config.network_node_url
    });
  }

  log.info('using RPC block fetcher');

  return new RpcBlockFetcher(config.network_node_url);
}
