import pino, { LoggerOptions, Logger as PinoLogger } from 'pino';

/** The minimum level to log. */
export enum LogLevel {
  /** Disable all logs.  */
  Silent = 'silent',
  /** Log unrecoverable errors. */
  Fatal = 'fatal',
  /** Log general errors. */
  Error = 'error',
  /** Log alerts or notices */
  Warn = 'warn',
  /** Log useful information. */
  Info = 'info',
  /** Log debug and trace information. */
  Debug = 'debug'
}

type Logger = Omit<PinoLogger, 'trace'>;

export const createLogger = (opts: LoggerOptions = {}): Logger => {
  return pino(
    opts,
    pino.destination({
      sync: true
    })
  );
};

// re-export types as it is.
export { Logger, LoggerOptions };
