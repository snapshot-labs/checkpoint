import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { HyperSyncEvmProvider } from './hyper-sync-provider';
import { Writer } from './types';

export class HyperSyncEvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private options: { apiToken: string };

  constructor(writers: Record<string, Writer>, options: { apiToken: string }) {
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
      apiToken: this.options.apiToken
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
