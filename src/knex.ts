import fs from 'fs';
import { ConnectionString } from 'connection-string';
import knex, { Knex } from 'knex';

export type KnexType =
  | {
      name: 'integer' | 'bigint' | 'boolean' | 'text' | 'json' | 'jsonb';
    }
  | {
      name: 'decimal';
      options: [number, number];
    }
  | {
      name: 'float';
      options: [number];
    }
  | {
      name: 'string';
      options: [number];
    };

const PROTOCOLS = {
  postgres: 'pg',
  postgresql: 'pg'
};

export function getConnectionData(connectionString: string) {
  const connectionConfig = new ConnectionString(connectionString);
  if (
    !connectionConfig.protocol ||
    !connectionConfig.hosts ||
    !connectionConfig.path
  ) {
    throw new Error('invalid connection string provided');
  }

  const client = PROTOCOLS[connectionConfig.protocol];
  if (!client) {
    throw new Error(
      `Supplied protocol ${connectionConfig.protocol} is not supported`
    );
  }

  const sslConfig: {
    rejectUnauthorized?: boolean;
    sslmode?: string;
    ca?: string;
  } = {};
  if (
    connectionConfig.params?.sslaccept === 'strict' ||
    connectionConfig.params?.ssl === 'rejectUnauthorized'
  ) {
    sslConfig.rejectUnauthorized = true;
  }
  if (connectionConfig.params?.sslmode) {
    sslConfig.sslmode = connectionConfig.params.sslmode;
  }

  if (process.env.CA_CERT) {
    sslConfig.ca = process.env.CA_CERT;
  } else if (process.env.CA_CERT_FILE) {
    sslConfig.ca = fs.readFileSync(process.env.CA_CERT_FILE).toString();
  }

  return {
    client,
    connection: {
      database: connectionConfig.path[0],
      user: connectionConfig.user,
      password: connectionConfig.password,
      host: connectionConfig.hosts[0].name,
      port: connectionConfig.hosts[0].port,
      ssl: Object.keys(sslConfig).length > 0 ? sslConfig : undefined
    }
  };
}

export function createKnexConfig(connectionString: string): Knex.Config {
  return getConnectionData(connectionString);
}

export function createKnex(config: string | Knex.Config) {
  const parsedConfig =
    typeof config === 'string' ? createKnexConfig(config) : config;

  return knex(parsedConfig);
}
