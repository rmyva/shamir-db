/**
 * Wire type model — the single home for ShamirDB's platform-agnostic DTO
 * types. This barrel re-exports only; every type lives in a sibling file
 * (filter.ts, query.ts, connection.ts). Constructor/builder CODE lives
 * under `../builders/`, kept apart from these declarations.
 *
 * PLATFORM-AGNOSTIC.
 */

export type {
  FieldPath,
  FilterValue,
  FnCall,
  ExprOp,
  FilterExprValue,
  CondValue,
  Filter,
  ComputedFilter,
} from './read/filter.js';

export type {
  TableRefWire,
  AggFunc,
  AggregateField,
  SelectItem,
  Select,
  GroupBy,
  OrderDirection,
  NullsOrder,
  OrderByItem,
  OrderBy,
  Pagination,
  At,
  Temporal,
  ReadQuery,
  PlanType,
  ExplainPlan,
} from './read/query.js';

export type { ConnectOptions, ScramConnectOptions, TokenConnectOptions, ConnectLocalOptions, ResumeOptions, ResumeLocalOptions, SessionTransport } from './connection.js';

export type {
  WireValue,
  WriteValue,
  UpdateReturnMode,
  UpdateSelect,
  InsertOp,
  UpdateOp,
  SetOp,
  DeleteOp,
  WriteOp,
} from './write/write.js';

export type {
  HmacSigner,
  Retention,
  BufferConfigDto,
  BufferConfigPatch,
  PurgeScope,
  WriteOpKind,
  NumDto,
  ConstraintsDto,
  ForeignKeyDto,
  FkAction,
  FieldRuleDto,
  SetTableSchemaOp,
  AddSchemaRuleOp,
  RemoveSchemaRuleOp,
  GetTableSchemaOp,
  CreateDbOp,
  DropDbOp,
  CreateRepoOp,
  AlterRepoOp,
  DescribeRepoOp,
  DescribeRepoResponse,
  DurabilityOverride,
  EffectiveDurability,
  WalCommit,
  WalRetentionExceeded,
  ReplicationServe,
  DropRepoOp,
  CreateTableOp,
  DropTableOp,
  CreateIndexOp,
  DropIndexOp,
  SetBufferConfigOp,
  GetBufferConfigOp,
  AlterBufferConfigOp,
  MigrationStatusOp,
  StartMigrationOp,
  CommitMigrationOp,
  RollbackMigrationOp,
  CreateFunctionOp,
  DropFunctionOp,
  RenameFunctionOp,
  CreateValidatorOp,
  DropValidatorOp,
  RenameValidatorOp,
  BindValidatorOp,
  UnbindValidatorOp,
  ListValidatorsOp,
  CreateFunctionFolderOp,
  RenameFunctionFolderOp,
  SetRetentionOp,
  PurgeHistoryOp,
  ChangesSinceOp,
  ListOp,
  DdlOp,
} from './schema/ddl.js';

export type {
  ApiTokenMode,
  ApiTokenChannels,
  CreateApiTokenSpec,
  CreateApiTokenOp,
  ListApiTokensOp,
  RevokeApiTokenOp,
  RevokeUserTokensOp,
  CreatedApiToken,
  ApiTokenInfo,
  ApiTokenAdmin,
} from './access/token-admin.js';

export type {
  ResourceRef,  GroupRef,
  Resource,
  Action,
  Effect,
  Permission,
  ChmodOp,
  ChownOp,
  ChgrpOp,
  CreateGroupOp,
  DropGroupOp,
  RenameGroupOp,
  AddGroupMemberOp,
  RemoveGroupMemberOp,
  AccessTreeOp,
  CreateUserOp,
  DropUserOp,
  GrantRoleOp,
  RevokeRoleOp,
  SetSuperuserOp,
  SetReplicatorOp,
  AdminOp,
} from './access/admin.js';

export type {
  ReplScope,
  ReplDirection,
  ReplMode,
  ReplStream,
  SubAction,
  CreateReplicationProfileOp,
  DropReplicationProfileOp,
  CreatePublicationOp,
  DropPublicationOp,
  CreateSubscriptionOp,
  DropSubscriptionOp,
  AlterSubscriptionOp,
  ListPublicationsOp,
  ListSubscriptionsOp,
  ReplicationStatusOp,
  ReplicationOp,
} from './stream/replication.js';

export type {
  CallOp,
} from './write/call.js';

export type {
  EventMask,
  SubscriptionSource,
  DeliverMode,
  SubscribeOp,
  UnsubscribeOp,
  PushKind,
  PushEnvelope,
} from './stream/subscribe.js';

export type {
  SubBatchOp,
  BatchOpInput,
  QueryEntry,
  IsolationLevel,
  DurabilityLevel,
  IndexVisibility,
  BatchLimits,
  BatchRequest,
  QueryStats,
  PaginationInfo,
  QueryResult,
  VisibilityWait,
  VisibilityState,
  VisibilityPendingReason,
  TransactionInfo,
  BatchResponse,
  EdgeKind,
} from './write/batch.js';

export type {
  CursorId,
  CreateCursorRequest,
  FetchNextRequest,
  CancelCursorRequest,
  CursorPageResponse,
  CursorClosedResponse,
} from './read/cursor.js';

