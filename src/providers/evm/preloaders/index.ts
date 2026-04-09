import { CheckpointConfig } from '../../../types';
import { Logger } from '../../../utils/logger';
import { FetchedBlock, Preloader } from './types';
import { HypersyncPreloader } from './hypersync';

export { FetchedBlock, Preloader, HypersyncPreloader };

export function createPreloader(
  config: CheckpointConfig,
  log: Logger
): Preloader | undefined {
  if (!config.hypersync_api_token) return;

  log.info('using HyperSync preloader');

  return new HypersyncPreloader({
    apiToken: config.hypersync_api_token,
    rpcUrl: config.network_node_url
  });
}
