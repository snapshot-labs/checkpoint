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
        user: 'root'
      }
    });
  });
});
