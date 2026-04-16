import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { HyperSyncEvmProvider } from './hypersync-provider';
import { Preloader, Writer } from './types';

export class HyperSyncEvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private preloaders: Record<string, Preloader>;
  private options: {
    apiToken: string;
    preloaders?: Record<string, Preloader>;
    preloadRange?: number;
  };

  constructor(
    writers: Record<string, Writer>,
    options: {
      apiToken: string;
      preloaders?: Record<string, Preloader>;
      preloadRange?: number;
    }
  ) {
    super();

    if (!options.apiToken) {
      throw new Error('HyperSync API token is required');
    }

    this.writers = writers;
    this.preloaders = options.preloaders ?? {};
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
      preloaders: this.preloaders,
      apiToken: this.options.apiToken,
      preloadRange: this.options.preloadRange
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }

  public getPreloaders(): string[] {
    return Object.keys(this.preloaders);
  }
}
