import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { HyperSyncEvmProvider } from './hypersync-provider';
import { Writer } from './types';

export class HyperSyncEvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private apiToken: string;

  constructor(writers: Record<string, Writer>, apiToken: string) {
    super();
    this.writers = writers;
    this.apiToken = apiToken;
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
      apiToken: this.apiToken
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
