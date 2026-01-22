/**
 * WebSocket BidRates Subscription Protocol
 *
 * Defines the message types and protocol for subscription-based bid statistics updates.
 * This protocol enables clients to subscribe to specific tasks and receive delta updates
 * instead of polling for full state.
 */

import { TaskBidData } from './task-metrics-types';

// ============================================================================
// Message Type Discriminators
// ============================================================================

export enum BidRatesMessageType {
  // Client → Server messages
  SUBSCRIBE = 'bidrates:subscribe',
  UNSUBSCRIBE = 'bidrates:unsubscribe',

  // Server → Client messages
  SUBSCRIBE_ACK = 'bidrates:subscribe_ack',
  UNSUBSCRIBE_ACK = 'bidrates:unsubscribe_ack',
  TASK_UPDATE = 'bidrates:task_update',
  GLOBAL_UPDATE = 'bidrates:global_update',
  HEARTBEAT = 'bidrates:heartbeat'
}

// ============================================================================
// Client → Server Messages
// ============================================================================

/**
 * Subscribe Request
 *
 * Client sends this to subscribe to updates for specific tasks.
 *
 * Example:
 * {
 *   type: 'bidrates:subscribe',
 *   taskIds: ['task1', 'task2', 'task3'],
 *   subscribeToGlobal: true,
 *   requestId: 'req-123'
 * }
 */
export interface SubscribeRequest {
  type: BidRatesMessageType.SUBSCRIBE;
  taskIds: string[];
  subscribeToGlobal?: boolean; // Subscribe to global bidRates/RPS updates
  requestId?: string; // Optional ID for tracking acknowledgment
}

/**
 * Unsubscribe Request
 *
 * Client sends this to unsubscribe from updates for specific tasks.
 *
 * Example:
 * {
 *   type: 'bidrates:unsubscribe',
 *   taskIds: ['task1', 'task2'],
 *   unsubscribeFromGlobal: true,
 *   requestId: 'req-124'
 * }
 */
export interface UnsubscribeRequest {
  type: BidRatesMessageType.UNSUBSCRIBE;
  taskIds: string[];
  unsubscribeFromGlobal?: boolean;
  requestId?: string;
}

// ============================================================================
// Server → Client Messages
// ============================================================================

/**
 * Subscribe Acknowledgment
 *
 * Server confirms subscription request.
 *
 * Example:
 * {
 *   type: 'bidrates:subscribe_ack',
 *   taskIds: ['task1', 'task2', 'task3'],
 *   subscribedToGlobal: true,
 *   success: true,
 *   requestId: 'req-123',
 *   timestamp: 1705603200000
 * }
 */
export interface SubscribeAck {
  type: BidRatesMessageType.SUBSCRIBE_ACK;
  taskIds: string[];
  subscribedToGlobal: boolean;
  success: boolean;
  error?: string; // Present if success=false
  requestId?: string;
  timestamp: number;
}

/**
 * Unsubscribe Acknowledgment
 *
 * Server confirms unsubscription request.
 *
 * Example:
 * {
 *   type: 'bidrates:unsubscribe_ack',
 *   taskIds: ['task1', 'task2'],
 *   unsubscribedFromGlobal: true,
 *   success: true,
 *   requestId: 'req-124',
 *   timestamp: 1705603210000
 * }
 */
export interface UnsubscribeAck {
  type: BidRatesMessageType.UNSUBSCRIBE_ACK;
  taskIds: string[];
  unsubscribedFromGlobal: boolean;
  success: boolean;
  error?: string;
  requestId?: string;
  timestamp: number;
}

/**
 * Task Update (Delta)
 *
 * Server sends delta updates for a specific task.
 * Only changed fields are included in the delta.
 *
 * Example:
 * {
 *   type: 'bidrates:task_update',
 *   taskId: 'task1',
 *   delta: {
 *     bidCounts: { opensea: 42, blur: 18, magiceden: 5 },
 *     floorPrices: { opensea: 0.5, blur: 0.48, magiceden: 0.52 }
 *   },
 *   timestamp: 1705603215000
 * }
 */
export interface TaskUpdateMessage {
  type: BidRatesMessageType.TASK_UPDATE;
  taskId: string;
  delta: Partial<TaskBidData>; // Only changed fields
  timestamp: number;
}

/**
 * Global Update
 *
 * Server sends complete global bidRates and RPS updates.
 * Sent to clients subscribed to global updates.
 *
 * Example:
 * {
 *   type: 'bidrates:global_update',
 *   bidRates: {
 *     opensea: { bidsPerSecond: 5.2, totalBids: 1523, windowPeriod: 60 },
 *     blur: { bidsPerSecond: 2.1, totalBids: 612, windowPeriod: 60 },
 *     magiceden: { bidsPerSecond: 1.8, totalBids: 421, windowPeriod: 60 }
 *   },
 *   rps: {
 *     currentRPS: 9.1,
 *     maxRPS: 100
 *   },
 *   timestamp: 1705603220000
 * }
 */
export interface GlobalUpdateMessage {
  type: BidRatesMessageType.GLOBAL_UPDATE;
  bidRates: {
    opensea: MarketplaceBidRate;
    blur: MarketplaceBidRate;
    magiceden: MarketplaceBidRate;
  };
  rps: {
    currentRPS: number;
    maxRPS: number;
  };
  timestamp: number;
}

export interface MarketplaceBidRate {
  bidsPerSecond: number;
  totalBids: number;
  windowPeriod: number;
}

/**
 * Heartbeat Message
 *
 * Server sends periodic heartbeat to maintain connection health.
 *
 * Example:
 * {
 *   type: 'bidrates:heartbeat',
 *   timestamp: 1705603225000,
 *   serverInstance: 'server-1'
 * }
 */
export interface HeartbeatMessage {
  type: BidRatesMessageType.HEARTBEAT;
  timestamp: number;
  serverInstance?: string;
}

// ============================================================================
// Union Types
// ============================================================================

/**
 * All possible messages from Client → Server
 */
export type ClientMessage =
  | SubscribeRequest
  | UnsubscribeRequest;

/**
 * All possible messages from Server → Client
 */
export type ServerMessage =
  | SubscribeAck
  | UnsubscribeAck
  | TaskUpdateMessage
  | GlobalUpdateMessage
  | HeartbeatMessage;

/**
 * Any bidrates protocol message
 */
export type BidRatesMessage = ClientMessage | ServerMessage;

// ============================================================================
// Type Guards
// ============================================================================

export function isSubscribeRequest(msg: any): msg is SubscribeRequest {
  return msg?.type === BidRatesMessageType.SUBSCRIBE;
}

export function isUnsubscribeRequest(msg: any): msg is UnsubscribeRequest {
  return msg?.type === BidRatesMessageType.UNSUBSCRIBE;
}

export function isTaskUpdateMessage(msg: any): msg is TaskUpdateMessage {
  return msg?.type === BidRatesMessageType.TASK_UPDATE;
}

export function isGlobalUpdateMessage(msg: any): msg is GlobalUpdateMessage {
  return msg?.type === BidRatesMessageType.GLOBAL_UPDATE;
}

export function isHeartbeatMessage(msg: any): msg is HeartbeatMessage {
  return msg?.type === BidRatesMessageType.HEARTBEAT;
}

export function isClientMessage(msg: any): msg is ClientMessage {
  return isSubscribeRequest(msg) || isUnsubscribeRequest(msg);
}

export function isServerMessage(msg: any): msg is ServerMessage {
  return (
    msg?.type === BidRatesMessageType.SUBSCRIBE_ACK ||
    msg?.type === BidRatesMessageType.UNSUBSCRIBE_ACK ||
    isTaskUpdateMessage(msg) ||
    isGlobalUpdateMessage(msg) ||
    isHeartbeatMessage(msg)
  );
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Create a subscribe request message
 */
export function createSubscribeRequest(
  taskIds: string[],
  subscribeToGlobal = false,
  requestId?: string
): SubscribeRequest {
  return {
    type: BidRatesMessageType.SUBSCRIBE,
    taskIds,
    subscribeToGlobal,
    requestId
  };
}

/**
 * Create an unsubscribe request message
 */
export function createUnsubscribeRequest(
  taskIds: string[],
  unsubscribeFromGlobal = false,
  requestId?: string
): UnsubscribeRequest {
  return {
    type: BidRatesMessageType.UNSUBSCRIBE,
    taskIds,
    unsubscribeFromGlobal,
    requestId
  };
}

/**
 * Create a subscribe acknowledgment message
 */
export function createSubscribeAck(
  taskIds: string[],
  subscribedToGlobal: boolean,
  success: boolean,
  requestId?: string,
  error?: string
): SubscribeAck {
  return {
    type: BidRatesMessageType.SUBSCRIBE_ACK,
    taskIds,
    subscribedToGlobal,
    success,
    error,
    requestId,
    timestamp: Date.now()
  };
}

/**
 * Create an unsubscribe acknowledgment message
 */
export function createUnsubscribeAck(
  taskIds: string[],
  unsubscribedFromGlobal: boolean,
  success: boolean,
  requestId?: string,
  error?: string
): UnsubscribeAck {
  return {
    type: BidRatesMessageType.UNSUBSCRIBE_ACK,
    taskIds,
    unsubscribedFromGlobal,
    success,
    error,
    requestId,
    timestamp: Date.now()
  };
}

/**
 * Create a task update message
 */
export function createTaskUpdateMessage(
  taskId: string,
  delta: Partial<TaskBidData>
): TaskUpdateMessage {
  return {
    type: BidRatesMessageType.TASK_UPDATE,
    taskId,
    delta,
    timestamp: Date.now()
  };
}

/**
 * Create a global update message
 */
export function createGlobalUpdateMessage(
  bidRates: GlobalUpdateMessage['bidRates'],
  rps: GlobalUpdateMessage['rps']
): GlobalUpdateMessage {
  return {
    type: BidRatesMessageType.GLOBAL_UPDATE,
    bidRates,
    rps,
    timestamp: Date.now()
  };
}

/**
 * Create a heartbeat message
 */
export function createHeartbeatMessage(serverInstance?: string): HeartbeatMessage {
  return {
    type: BidRatesMessageType.HEARTBEAT,
    timestamp: Date.now(),
    serverInstance
  };
}
