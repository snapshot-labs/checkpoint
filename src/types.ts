import pino from 'pino';
import { z } from 'zod';
import { Instance } from './providers';
import {
  checkpointConfigSchema,
  contractSourceConfigSchema,
  contractTemplateSchema,
  overridesConfigSchema
} from './schemas';
import { LogLevel } from './utils/logger';

export type TemplateSource = {
  contractAddress: string;
  startBlock: number;
  template: string;
};

export interface CheckpointOptions {
  /** Setting this to true will trigger reset of database on config changes. */
  resetOnConfigChange?: boolean;
  /**
   * Set the log output levels for checkpoint. Defaults to Error.
   * Note, this does not affect the log outputs in writers.
   */
  logLevel?: LogLevel;
  /** Format logs to pretty output. Not recommended for production. */
  prettifyLogs?: boolean;
  /**
   * Custom pino options helpful when using custom transports.
   * Those options will be merged with Checkpoint's pino options.
   */
  pinoOptions?: pino.LoggerOptions;
  /**
   * Optional database connection string. Must be PostgreSQL connection string.
   * If not provided connection strinng will be read from DATABASE_URL environment variable.
   */
  dbConnection?: string;
  /** Overrides for database types. */
  overridesConfig?: OverridesConfig;
  /**
   * Skip fetching blocks from the network.
   * This can speed up indexing process if you don't need block data.
   */
  skipBlockFetching?: boolean;
}

export type ContractSourceConfig = z.infer<typeof contractSourceConfigSchema>;
export type ContractTemplate = z.infer<typeof contractTemplateSchema>;
export type CheckpointConfig = z.infer<typeof checkpointConfigSchema>;
export type OverridesConfig = z.infer<typeof overridesConfigSchema>;

export type BaseWriterParams = {
  blockNumber: number;
  source?: ContractSourceConfig;
  helpers: ReturnType<Instance['getWriterHelpers']>;
};
