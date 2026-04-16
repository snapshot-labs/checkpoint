import { StarknetProvider } from '.';
import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance } from '../base';
import { Preloader, Writer } from './types';

export class StarknetIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private preloaders: Record<string, Preloader>;

  constructor(
    writers: Record<string, Writer>,
    preloaders: Record<string, Preloader> = {}
  ) {
    super();
    this.writers = writers;
    this.preloaders = preloaders;
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
    this.provider = new StarknetProvider({
      instance,
      log,
      abis,
      writers: this.writers,
      preloaders: this.preloaders
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }

  public getPreloaders(): string[] {
    return Object.keys(this.preloaders);
  }
}
