/**
 * Handler Adapter for Cursor-Based Pull Architecture
 *
 * Adapts the pull-based normalized events to work with existing
 * counterbid handlers that expect push-based WebSocket message format.
 *
 * Key responsibilities:
 * - Create task matcher function for filtering events
 * - Convert NormalizedBidEvent back to expected handler formats
 * - Route events to appropriate marketplace handlers
 */

import { NormalizedBidEvent } from './types';
import { debugLog, warnLog, errorLog } from '../utils/debug-file-logger';
import { ITask } from '../models/task.model';

// Type aliases for the adapter
type TaskIndexGetter = (contractAddress?: string, slug?: string) => ITask[];

type CounterbidHandler = (data: any, task: ITask) => Promise<void>;

interface HandlerMap {
  opensea: CounterbidHandler;
  magiceden: CounterbidHandler;
  blur: CounterbidHandler;
}

/**
 * Create a task matcher function that filters events based on active tasks
 *
 * Uses the same O(1) lookup logic as the existing getRelevantTasks()
 *
 * @param getRelevantTasks - Function to get tasks for a contract/slug
 * @returns Function that returns true if any task cares about this event
 */
export function createTaskMatcher(
  getRelevantTasks: TaskIndexGetter
): (event: NormalizedBidEvent) => boolean {
  return (event: NormalizedBidEvent): boolean => {
    // Get tasks that match this event's contract or slug
    const tasks = getRelevantTasks(event.contractAddress, event.slug);

    if (tasks.length === 0) {
      return false;
    }

    // Check if any task is running and has this marketplace selected
    const marketplace = event.marketplace.toLowerCase();
    return tasks.some(task =>
      task.running &&
      task.selectedMarketplaces.some(m => m.toLowerCase() === marketplace)
    );
  };
}

/**
 * Reconstruct OpenSea message format from NormalizedBidEvent
 *
 * OpenSea messages use Phoenix protocol format with nested payload
 */
function reconstructOpenSeaMessage(event: NormalizedBidEvent): any {
  // The rawPayload should contain the original message structure
  // If available, use it; otherwise construct a minimal format
  if (event.rawPayload) {
    return {
      marketplace: 'opensea',
      ...(event.rawPayload as object),
      // Ensure these fields are set for routing
      _pullEvent: true,
      _seq: event.seq,
    };
  }

  // Construct minimal format for handlers to work
  // Note: The handlers expect message.payload.payload structure
  return {
    marketplace: 'opensea',
    event: event.eventType === 'bid_created' ? 'item_received_bid' :
           event.eventType === 'trait_offer' ? 'trait_offer' :
           event.eventType === 'collection_offer' ? 'collection_offer' :
           event.eventType,
    payload: {
      payload: {
        order_hash: event.orderHash,
        maker: { address: event.maker },
        base_price: event.priceWei,
        protocol_data: {
          parameters: {
            consideration: [{
              identifierOrCriteria: event.tokenId || '0',
            }],
            offer: [{
              token: event.currency,
            }],
          },
        },
        // Trait info for trait offers
        trait_criteria: event.traitType ? {
          trait_type: event.traitType,
          trait_value: event.traitValue,
        } : undefined,
        // Collection info
        collection: {
          slug: event.slug,
        },
        item: {
          nft: {
            address: event.contractAddress,
            identifier: event.tokenId,
          },
        },
      },
    },
    _pullEvent: true,
    _seq: event.seq,
  };
}

/**
 * Reconstruct MagicEden message format from NormalizedBidEvent
 *
 * MagicEden uses JSON format with data wrapper
 */
function reconstructMagicEdenMessage(event: NormalizedBidEvent): any {
  if (event.rawPayload) {
    return {
      marketplace: 'magiceden',
      ...(event.rawPayload as object),
      _pullEvent: true,
      _seq: event.seq,
    };
  }

  // Construct minimal format
  return {
    marketplace: 'magiceden',
    data: {
      id: event.orderHash,
      maker: event.maker,
      price: {
        amount: {
          raw: event.priceWei,
          decimal: event.priceEth,
        },
        currency: {
          contract: event.currency,
          decimals: event.currencyDecimals,
        },
      },
      criteria: {
        kind: event.tokenId ? 'token' :
              event.traitType ? 'attribute' :
              'collection',
        data: {
          token: event.tokenId ? {
            tokenId: event.tokenId,
          } : undefined,
          attribute: event.traitType ? {
            key: event.traitType,
            value: event.traitValue,
          } : undefined,
          collection: {
            id: event.slug,
          },
        },
      },
      contract: event.contractAddress,
    },
    _pullEvent: true,
    _seq: event.seq,
  };
}

/**
 * Reconstruct Blur message format from NormalizedBidEvent
 *
 * Blur uses Socket.io format with data in ['1'] key.
 * The rawPayload is the full transformed Socket.io message (array with eventType added),
 * so we extract [1] which contains the actual data with stats/updates.
 */
function reconstructBlurMessage(event: NormalizedBidEvent): any {
  if (event.rawPayload) {
    // Extract the inner data from Socket.io array format [channel, data]
    // rawPayload is the transformed message which is array-like with numeric keys
    const rawArray = event.rawPayload as any;
    const innerData = rawArray[1] || rawArray['1'] || rawArray;

    return {
      marketplace: 'blur',
      // Blur handlers expect data in ['1'] key with stats/updates
      '1': innerData,
      _pullEvent: true,
      _seq: event.seq,
    };
  }

  // Construct minimal format - Blur handler reads from message['1']
  return {
    marketplace: 'blur',
    '1': {
      orderHash: event.orderHash,
      maker: event.maker,
      price: event.priceWei,
      contractAddress: event.contractAddress,
      slug: event.slug,
      tokenId: event.tokenId,
      // Trait info
      traitType: event.traitType,
      traitValue: event.traitValue,
    },
    _pullEvent: true,
    _seq: event.seq,
  };
}

/**
 * Reconstruct message in expected format for handlers
 */
function reconstructMessage(event: NormalizedBidEvent): any {
  switch (event.marketplace) {
    case 'opensea':
      return reconstructOpenSeaMessage(event);
    case 'magiceden':
      return reconstructMagicEdenMessage(event);
    case 'blur':
      return reconstructBlurMessage(event);
    default:
      warnLog('PullAdapter', `Unknown marketplace: ${event.marketplace}`);
      return null;
  }
}

/**
 * Create an event handler that routes normalized events to marketplace handlers
 *
 * @param handlers - Map of marketplace handlers
 * @param getRelevantTasks - Function to get tasks for a contract/slug
 * @returns Async function that processes a NormalizedBidEvent
 */
export function createEventHandler(
  handlers: HandlerMap,
  getRelevantTasks: TaskIndexGetter
): (event: NormalizedBidEvent) => Promise<void> {
  return async (event: NormalizedBidEvent): Promise<void> => {
    try {
      // Get relevant tasks for this event
      const tasks = getRelevantTasks(event.contractAddress, event.slug);

      if (tasks.length === 0) {
        debugLog('PullAdapter', `No tasks for event | seq=${event.seq} | slug=${event.slug}`);
        return;
      }

      // Reconstruct message in expected format
      const message = reconstructMessage(event);
      if (!message) {
        return;
      }

      // Process each relevant task
      const marketplace = event.marketplace.toLowerCase();
      const handler = handlers[marketplace as keyof HandlerMap];

      if (!handler) {
        warnLog('PullAdapter', `No handler for marketplace: ${marketplace}`);
        return;
      }

      // Process tasks in parallel
      await Promise.all(
        tasks
          .filter(task =>
            task.running &&
            task.selectedMarketplaces.some(m => m.toLowerCase() === marketplace)
          )
          .map(async (task) => {
            try {
              // For Blur, handlers expect the nested data
              const data = marketplace === 'blur' ? message['1'] : message;
              await handler(data, task);
            } catch (error) {
              errorLog('PullAdapter', `Handler error | task=${task._id} | seq=${event.seq}`, error as Error);
            }
          })
      );

      debugLog('PullAdapter', `Processed event | seq=${event.seq} | tasks=${tasks.length} | marketplace=${marketplace}`);

    } catch (error) {
      errorLog('PullAdapter', `Failed to process event | seq=${event.seq}`, error as Error);
    }
  };
}

/**
 * Adapt existing counterbid handlers for use with pull consumer
 *
 * This wraps the existing handlers to ensure they receive data in the
 * expected format, even when coming from normalized pull events.
 */
export function adaptHandler(
  handler: CounterbidHandler
): CounterbidHandler {
  return async (data: any, task: ITask): Promise<void> => {
    // Log pull event processing
    if (data._pullEvent) {
      debugLog('PullAdapter', `Processing pull event | seq=${data._seq} | task=${task._id}`);
    }

    // Call original handler
    await handler(data, task);
  };
}

/**
 * Create wrapped handlers that work with pull events
 */
export function createAdaptedHandlers(
  openseaHandler: CounterbidHandler,
  magicedenHandler: CounterbidHandler,
  blurHandler: CounterbidHandler
): HandlerMap {
  return {
    opensea: adaptHandler(openseaHandler),
    magiceden: adaptHandler(magicedenHandler),
    blur: adaptHandler(blurHandler),
  };
}
