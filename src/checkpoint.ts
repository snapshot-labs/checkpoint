import { Knex } from 'knex';
import { Pool as PgPool } from 'pg';
import { Container } from './container';
import getGraphQL from './graphql';
import { GqlEntityController } from './graphql/controller';
import { createKnex } from './knex';
import { createPgPool } from './pg';
import { BaseIndexer } from './providers';
import { register } from './register';
import { checkpointConfigSchema } from './schemas';
import { CheckpointsStore } from './stores/checkpoints';
import { CheckpointConfig, CheckpointOptions } from './types';
import { extendSchema } from './utils/graphql';
import { createLogger, Logger, LogLevel } from './utils/logger';

export default class Checkpoint {
  private readonly entityController: GqlEntityController;
  private readonly log: Logger;

  private containers: Map<string, Container> = new Map();

  private schema: string;
  private dbConnection: string;
  private knex: Knex;
  private pgPool?: PgPool;
  private checkpointsStore?: CheckpointsStore;
  private opts?: CheckpointOptions;

  constructor(schema: string, opts?: CheckpointOptions) {
    this.schema = extendSchema(schema);
    this.entityController = new GqlEntityController(
      this.schema,
      opts?.overridesConfig
    );

    this.opts = opts;
    this.log = createLogger({
      base: { component: 'checkpoint' },
      level: opts?.logLevel || LogLevel.Error,
      ...(opts?.prettifyLogs
        ? {
            transport: {
              target: 'pino-pretty'
            }
          }
        : {}),
      ...opts?.pinoOptions
    });

    const dbConnection = opts?.dbConnection || process.env.DATABASE_URL;
    if (!dbConnection) {
      throw new Error(
        'a valid connection string or DATABASE_URL environment variable is required to connect to the database'
      );
    }

    this.knex = createKnex(dbConnection);
    this.dbConnection = dbConnection;

    register.setKnex(this.knex);
  }

  public addIndexer(
    name: string,
    config: CheckpointConfig,
    indexer: BaseIndexer
  ) {
    const validationResult = checkpointConfigSchema.safeParse(config);
    if (validationResult.success === false) {
      throw new Error(
        `Checkpoint config is invalid: ${validationResult.error.message}`
      );
    }

    const container = new Container(
      name,
      this.log,
      this.knex,
      this.store,
      this.entityController,
      config,
      indexer,
      this.schema,
      this.opts
    );

    container.validateConfig();

    this.containers.set(name, container);
  }

  public getBaseContext() {
    return {
      log: this.log.child({ component: 'resolver' }),
      knex: this.knex,
      pg: this.pg
    };
  }

  public getSchema() {
    return this.entityController.generateSchema({ addResolvers: true });
  }

  /**
   * Returns an express handler that exposes a GraphQL API to query entities defined
   * in the schema.
   *
   */
  public get graphql() {
    const schema = this.getSchema();

    return getGraphQL(
      schema,
      this.getBaseContext(),
      this.entityController.generateSampleQuery()
    );
  }

  /**
   * Starts the indexer.
   *
   * The indexer will invoker the respective writer functions when a contract
   * event is found.
   *
   */
  public async start() {
    this.log.debug('starting');

    await Promise.all(
      [...this.containers.values()].map(container => container.start())
    );
  }

  /**
   * Reset will clear the last synced block informations
   * and force Checkpoint to start indexing from the start
   * block.
   *
   * This will also clear all indexed GraphQL entity records.
   *
   * This should be called when there has been a change to the GraphQL schema
   * or a change to the writer functions logic, so indexing will re-run from
   * the starting block. Also, it should be called the first time Checkpoint
   * is being initialized.
   *
   */
  public async reset() {
    this.log.debug('reset');
    await this.store.createStore();

    for (const container of this.containers.values()) {
      await container.reset();
    }

    await this.entityController.createEntityStores(this.knex);
  }

  /**
   * Resets Checkpoint's internal tables (including checkpoints).
   *
   * Calling this function will cause next run of checkpoint to start syncing
   * from the start, block-by-block, until new checkpoints are found.
   *
   */
  public async resetMetadata() {
    this.log.debug('reset metadata');

    await this.store.resetStore();

    for (const container of this.containers.values()) {
      await container.resetMetadata();
    }
  }

  /**
   * Registers the blocks where a contracts event can be found.
   * This will be used as a skip list for checkpoints while
   * indexing relevant blocks. Using this seed function can significantly
   * reduce the time for Checkpoint to re-index blocks.
   *
   * This should be called before the start() method is called.
   *
   */
  public async seedCheckpoints(
    indexerName: string,
    checkpointBlocks: { contract: string; blocks: number[] }[]
  ): Promise<void> {
    await this.store.createStore();

    const container = this.containers.get(indexerName);

    if (!container) {
      throw new Error(`Container ${indexerName} not found`);
    }

    container.seedCheckpoints(checkpointBlocks);
  }

  private get store(): CheckpointsStore {
    if (this.checkpointsStore) {
      return this.checkpointsStore;
    }

    return (this.checkpointsStore = new CheckpointsStore(this.knex, this.log));
  }

  private get pg(): PgPool {
    if (this.pgPool) {
      return this.pgPool;
    }

    this.pgPool = createPgPool(this.dbConnection);
    return this.pgPool;
  }
}
