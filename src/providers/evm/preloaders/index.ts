import { HypersyncPreloader } from './hypersync';
import { FetchedBlock, Preloader } from './types';
import { CheckpointConfig } from '../../../types';
import { Logger } from '../../../utils/logger';

export { FetchedBlock, Preloader, HypersyncPreloader };

export function createPreloader(
  config: CheckpointConfig,
  chainId: number,
  log: Logger
): Preloader | undefined {
  if (!config.hypersync_api_token) return;

  log.info('using HyperSync preloader');

  return new HypersyncPreloader({
    apiToken: config.hypersync_api_token,
    chainId
  });
}
