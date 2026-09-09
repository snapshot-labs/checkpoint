import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance, RpcSelector } from '../base';
import { HyperSyncEvmProvider } from './hypersync-provider';
import { Writer } from './types';

export class HyperSyncEvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private options: { apiToken: string; rpcSelector?: RpcSelector };

  constructor(
    writers: Record<string, Writer>,
    options: { apiToken: string; rpcSelector?: RpcSelector }
  ) {
    super();

    if (!options.apiToken) {
      throw new Error('HyperSync API token is required');
    }

    this.writers = writers;
    this.options = options;
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
    log.info('using HyperSync provider');

    this.provider = new HyperSyncEvmProvider({
      instance,
      log,
      abis,
      writers: this.writers,
      apiToken: this.options.apiToken,
      rpcSelector: this.options.rpcSelector
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
