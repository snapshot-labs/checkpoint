import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { createFetchers } from './fetchers';
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
    const { fetcher, preloader } = createFetchers(instance.config, log);

    this.provider = new EvmProvider({
      instance,
      log,
      abis,
      writers: this.writers,
      fetcher,
      preloader
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
