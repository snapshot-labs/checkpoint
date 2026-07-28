import { describe, expect, it } from 'bun:test';
import { createKnexConfig } from '../../src/knex';

describe('createKnexConfig', () => {
  it('should create knex config', () => {
    expect(
      createKnexConfig(
        'postgres://root:default_password@localhost:3306/checkpoint'
      )
    ).toEqual({
      client: 'pg',
      connection: {
        database: 'checkpoint',
        host: 'localhost',
        password: 'default_password',
        port: 3306,
        ssl: undefined,
        user: 'root',
        keepAlive: true,
        keepAliveInitialDelayMillis: 10_000,
        connectionTimeoutMillis: 30_000,
        query_timeout: 60_000
      }
    });
  });
});
