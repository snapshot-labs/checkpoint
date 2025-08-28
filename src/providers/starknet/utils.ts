import { Abi, CallData, Event, events, ParsedEvent } from 'starknet';

const convertEvent = (input: any) => {
  if (typeof input === 'bigint') return `0x${input.toString(16)}`;
  if (Array.isArray(input)) return input.map(convertEvent);
  if (typeof input === 'object')
    return Object.fromEntries(
      Object.entries(input).map(([k, v]) => [k, convertEvent(v)])
    );

  return input;
};

export const parseEvent = (abi: Abi, event: Event): ParsedEvent => {
  const abiEvents = events.getAbiEvents(abi);
  const structs = CallData.getAbiStruct(abi);
  const enums = CallData.getAbiEnum(abi);

  const parsedEvents = events.parseEvents([event], abiEvents, structs, enums);
  if (parsedEvents.length === 0) throw new Error('Failed to parse event');

  const parsedEvent = parsedEvents[0];
  const key = Object.keys(parsedEvent)[0];

  return convertEvent(parsedEvent[key]);
};
