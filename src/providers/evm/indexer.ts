import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { HypersyncPreloader } from './fetchers/hypersync';
import { EvmProvider } from './provider';
import { Writer } from './types';

export class EvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;

  constructor(writers: Record<string, Writer>) {
    super();
    this.writers = writers;
  }

  init({
    instance,
    log,
    abis
  }: {
    instance: Instance;
    log: Logger;
    abis?: Record<string, any>;
  }) {
    let preloader;
    if (instance.config.hypersync_api_token) {
      log.info('using HyperSync preloader');
      preloader = new HypersyncPreloader({
        apiToken: instance.config.hypersync_api_token,
        rpcUrl: instance.config.network_node_url
      });
    }

    this.provider = new EvmProvider({
      instance,
      log,
      abis,
      writers: this.writers,
      preloader
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
