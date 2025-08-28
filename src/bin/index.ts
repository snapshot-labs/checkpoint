#!/usr/bin/env node

import fs from 'fs/promises';
import path from 'path';
import process from 'process';
import { printSchema } from 'graphql';
import { hideBin } from 'yargs/helpers';
import yargs from 'yargs/yargs';
import { codegen } from '../codegen';
import { GqlEntityController } from '../graphql/controller';
import { OverridesConfig } from '../types';
import { extendSchema } from '../utils/graphql';

const DEFAULT_CONFIG_PATH = 'src/overrides.json';
const DEFAULT_SCHEMA_PATH = 'src/schema.gql';
const OUTPUT_DIRECTORY = '.checkpoint';

async function generate(
  schemaFile: string,
  overridesConfigFile: string,
  format: string
) {
  if (format !== 'typescript' && format !== 'javascript') {
    throw new Error('Invalid output format');
  }

  const cwd = process.cwd();
  const schemaFilePath = path.join(cwd, schemaFile);
  const overridesConfigFilePath = path.join(cwd, overridesConfigFile);

  console.log('Generating models from schema:', schemaFile);

  let config: OverridesConfig = {};
  try {
    config = await import(overridesConfigFilePath);
  } catch {}

  let schema = await fs.readFile(schemaFilePath, 'utf8');
  schema = extendSchema(schema);

  const controller = new GqlEntityController(schema, config);

  const generatedModels = codegen(controller, config, format);

  const outputFile = format === 'typescript' ? 'models.ts' : 'models.js';
  const modelsOutputPath = path.join(OUTPUT_DIRECTORY, outputFile);

  await fs.mkdir(path.join(cwd, OUTPUT_DIRECTORY), { recursive: true });
  await fs.writeFile(path.join(cwd, modelsOutputPath), generatedModels);

  console.log('Models generated to', modelsOutputPath);

  console.log('Generating query schema');
  const querySchema = controller.generateSchema();
  const schemaOutputPath = path.join(OUTPUT_DIRECTORY, 'schema.gql');
  await fs.writeFile(
    path.join(cwd, schemaOutputPath),
    printSchema(querySchema)
  );

  console.log('Schema generated to', schemaOutputPath);
}

yargs(hideBin(process.argv))
  .command(
    'generate',
    'generate models from schema',
    yargs => {
      return yargs
        .option('schema-file', {
          alias: 's',
          type: 'string',
          default: DEFAULT_SCHEMA_PATH,
          description: 'Schema file path'
        })
        .option('overrides-config-file', {
          alias: 'o',
          type: 'string',
          default: DEFAULT_CONFIG_PATH,
          description: 'Overrides config file path'
        })
        .option('output-format', {
          alias: 'f',
          type: 'string',
          default: 'typescript',
          description: 'Output format (typescript or javascript)'
        });
    },
    async argv => {
      try {
        await generate(
          argv['schema-file'],
          argv['overrides-config-file'],
          argv['output-format']
        );
      } catch (err) {
        console.error('Error generating models:', err);
        process.exit(1);
      }
    }
  )
  .demandCommand(1, 'You need to specify a command')
  .parse();
