export type { BlockFetcher, FetchedBlock } from './types';
export { RpcBlockFetcher } from './rpc';
export { HypersyncBlockFetcher } from './hypersync';

import { CheckpointConfig } from '../../../types';
import { Logger } from '../../../utils/logger';
import { BlockFetcher } from './types';
import { RpcBlockFetcher } from './rpc';
import { HypersyncBlockFetcher } from './hypersync';

export async function createBlockFetcher(
  config: CheckpointConfig,
  log: Logger
): Promise<BlockFetcher> {
  if (config.hypersync_api_token) {
    const rpcFetcher = new RpcBlockFetcher(config.network_node_url);
    const chainId = await rpcFetcher.getChainId();

    log.info({ chainId }, 'using HyperSync block fetcher');

    return new HypersyncBlockFetcher({
      chainId,
      apiToken: config.hypersync_api_token,
      rpcUrl: config.network_node_url
    });
  }

  log.info('using RPC block fetcher');

  return new RpcBlockFetcher(config.network_node_url);
}
