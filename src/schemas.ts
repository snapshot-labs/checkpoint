import { z } from 'zod';

export const contractEventConfigSchema = z.object({
  name: z.string(),
  fn: z.string()
});

export const contractSourceConfigSchema = z.object({
  contract: z.string(),
  abi: z.string().optional(),
  start: z.number().gte(0),
  events: z.array(contractEventConfigSchema)
});

export const contractTemplateSchema = z.object({
  abi: z.string().optional(),
  events: z.array(contractEventConfigSchema)
});

export const checkpointConfigSchema = z.object({
  network_node_url: z.string().url(),
  optimistic_indexing: z.boolean().optional(),
  fetch_interval: z.number().optional(),
  start: z.number().gte(0).optional(),
  tx_fn: z.string().optional(),
  global_events: z.array(contractEventConfigSchema).optional(),
  sources: z.array(contractSourceConfigSchema).optional(),
  templates: z.record(contractTemplateSchema).optional(),
  abis: z.record(z.any()).optional()
});

export const overridesConfigSchema = z.object({
  /** Decimal types to define for use in your schema. */
  decimal_types: z
    .record(
      z.object({
        p: z.number(),
        d: z.number()
      })
    )
    .optional()
});
