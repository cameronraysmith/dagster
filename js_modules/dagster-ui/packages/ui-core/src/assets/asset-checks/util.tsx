import {Colors, Icon, IconName, Spinner} from '@dagster-io/ui-components';

import {ExecuteChecksButtonCheckFragment} from './types/ExecuteChecksButton.types';
import {AssetCheckTableFragment} from './types/VirtualizedAssetCheckTable.types';
import {assertUnreachable} from '../../app/Util';
import {AssetCheckLiveFragment} from '../../asset-data/types/AssetBaseDataProvider.types';
import {AssetCheckExecutionResolvedStatus, AssetCheckSeverity} from '../../graphql/types';

// Type for checks that have partition status information
type AssetCheckWithPartitionStatuses = Pick<
  AssetCheckLiveFragment,
  'executionForLatestMaterialization' | 'partitionStatuses'
>;

export function assetCheckStatusDescription(
  check: AssetCheckTableFragment & ExecuteChecksButtonCheckFragment,
) {
  const lastExecution = check.executionForLatestMaterialization;
  if (!lastExecution) {
    return 'Not evaluated';
  }
  const status = lastExecution.status;
  switch (status) {
    case AssetCheckExecutionResolvedStatus.EXECUTION_FAILED:
      return 'Execution failed';
    case AssetCheckExecutionResolvedStatus.FAILED:
      return 'Failed';
    case AssetCheckExecutionResolvedStatus.IN_PROGRESS:
      return 'In progress';
    case AssetCheckExecutionResolvedStatus.SKIPPED:
      return 'Skipped';
    case AssetCheckExecutionResolvedStatus.SUCCEEDED:
      return 'Succeeded';
    default:
      assertUnreachable(status);
  }
}

export function getCheckIcon(
  check: AssetCheckTableFragment & ExecuteChecksButtonCheckFragment,
): React.ReactNode {
  const lastExecution = check.executionForLatestMaterialization;
  if (!lastExecution) {
    return <Icon name="status" color={Colors.accentGray()} />;
  }
  const status = lastExecution.status;
  const isWarning = lastExecution.evaluation?.severity === AssetCheckSeverity.WARN;
  switch (status) {
    case AssetCheckExecutionResolvedStatus.EXECUTION_FAILED:
      return (
        <Icon name="sync_problem" color={isWarning ? Colors.accentYellow() : Colors.accentRed()} />
      );
    case AssetCheckExecutionResolvedStatus.FAILED:
      if (isWarning) {
        return <Icon name="warning_outline" color={Colors.accentYellow()} />;
      }
      return <Icon name="cancel" color={Colors.accentRed()} />;
    case AssetCheckExecutionResolvedStatus.IN_PROGRESS:
      return <Spinner purpose="body-text" />;
    case AssetCheckExecutionResolvedStatus.SKIPPED:
      return <Icon name="dot" />;
    case AssetCheckExecutionResolvedStatus.SUCCEEDED:
      return <Icon name="check_circle" color={Colors.accentGreen()} />;
    default:
      assertUnreachable(status);
  }
}

export function assetCheckExecutionStatusText(status: AssetCheckExecutionResolvedStatus): string {
  switch (status) {
    case AssetCheckExecutionResolvedStatus.EXECUTION_FAILED:
      return 'Execution failed';
    case AssetCheckExecutionResolvedStatus.FAILED:
      return 'Failed';
    case AssetCheckExecutionResolvedStatus.IN_PROGRESS:
      return 'In progress';
    case AssetCheckExecutionResolvedStatus.SKIPPED:
      return 'Skipped';
    case AssetCheckExecutionResolvedStatus.SUCCEEDED:
      return 'Succeeded';
    default:
      assertUnreachable(status);
  }
}

export function assetCheckExecutionStatusIcon(status: AssetCheckExecutionResolvedStatus): IconName {
  switch (status) {
    case AssetCheckExecutionResolvedStatus.EXECUTION_FAILED:
      return 'sync_problem';
    case AssetCheckExecutionResolvedStatus.FAILED:
      return 'cancel';
    case AssetCheckExecutionResolvedStatus.IN_PROGRESS:
      return 'hourglass_bottom';
    case AssetCheckExecutionResolvedStatus.SKIPPED:
      return 'dot';
    case AssetCheckExecutionResolvedStatus.SUCCEEDED:
      return 'check_circle';
    default:
      assertUnreachable(status);
  }
}

export type AssetCheckIconType =
  | AssetCheckExecutionResolvedStatus
  | 'ERROR'
  | 'WARN'
  | 'NOT_EVALUATED';

export interface AssetCheckPartitionStats {
  numSucceeded: number;
  numFailed: number;
  numExecutionFailed: number;
  numInProgress: number;
  numSkipped: number;
}

/**
 * Calculate aggregate partition statistics for a check.
 * Returns null if the check is not partitioned.
 */
export function getCheckPartitionStats(
  check: AssetCheckWithPartitionStatuses,
): AssetCheckPartitionStats | null {
  if (!check.partitionStatuses) {
    return null;
  }

  const stats: AssetCheckPartitionStats = {
    numSucceeded: 0,
    numFailed: 0,
    numExecutionFailed: 0,
    numInProgress: 0,
    numSkipped: 0,
  };

  const statuses = check.partitionStatuses;

  if (statuses.__typename === 'AssetCheckDefaultPartitionStatuses') {
    stats.numSucceeded = statuses.succeededPartitions?.length || 0;
    stats.numFailed = statuses.failedPartitions?.length || 0;
    stats.numExecutionFailed = statuses.executionFailedPartitions?.length || 0;
    stats.numInProgress = statuses.inProgressPartitions?.length || 0;
    stats.numSkipped = statuses.skippedPartitions?.length || 0;
  } else if (statuses.__typename === 'AssetCheckTimePartitionStatuses') {
    // Count partitions across ranges
    statuses.ranges?.forEach((range) => {
      const rangeLength = 1; // Simplified - can be enhanced
      switch (range.status) {
        case 'SUCCEEDED':
          stats.numSucceeded += rangeLength;
          break;
        case 'FAILED':
          stats.numFailed += rangeLength;
          break;
        case 'EXECUTION_FAILED':
          stats.numExecutionFailed += rangeLength;
          break;
        case 'IN_PROGRESS':
          stats.numInProgress += rangeLength;
          break;
        case 'SKIPPED':
          stats.numSkipped += rangeLength;
          break;
      }
    });
  } else if (statuses.__typename === 'AssetCheckMultiPartitionStatuses') {
    // Handle multi-partition by flattening secondary dimensions
    statuses.ranges?.forEach((range) => {
      const secondaryDim = range.secondaryDim;
      if (secondaryDim.__typename === 'AssetCheckDefaultPartitionStatuses') {
        stats.numSucceeded += secondaryDim.succeededPartitions?.length || 0;
        stats.numFailed += secondaryDim.failedPartitions?.length || 0;
        stats.numExecutionFailed += secondaryDim.executionFailedPartitions?.length || 0;
        stats.numInProgress += secondaryDim.inProgressPartitions?.length || 0;
        stats.numSkipped += secondaryDim.skippedPartitions?.length || 0;
      } else if (secondaryDim.__typename === 'AssetCheckTimePartitionStatuses') {
        secondaryDim.ranges?.forEach((r) => {
          const rangeLength = 1;
          switch (r.status) {
            case 'SUCCEEDED':
              stats.numSucceeded += rangeLength;
              break;
            case 'FAILED':
              stats.numFailed += rangeLength;
              break;
            case 'EXECUTION_FAILED':
              stats.numExecutionFailed += rangeLength;
              break;
            case 'IN_PROGRESS':
              stats.numInProgress += rangeLength;
              break;
            case 'SKIPPED':
              stats.numSkipped += rangeLength;
              break;
          }
        });
      }
    });
  }

  return stats;
}

/**
 * Get aggregate icon type for a check (handles partitioned checks).
 * Follows priority: ERROR > WARN > IN_PROGRESS > SKIPPED > SUCCEEDED > NOT_EVALUATED
 */
export function getAggregateCheckIconType(
  check: AssetCheckWithPartitionStatuses,
): AssetCheckIconType {
  const partitionStats = getCheckPartitionStats(check);

  if (!partitionStats) {
    // Fall back to single execution behavior
    const status = check.executionForLatestMaterialization?.status;
    return status === undefined
      ? 'NOT_EVALUATED'
      : status === AssetCheckExecutionResolvedStatus.FAILED
        ? check.executionForLatestMaterialization?.evaluation?.severity === AssetCheckSeverity.WARN
          ? 'WARN'
          : 'ERROR'
        : status === AssetCheckExecutionResolvedStatus.EXECUTION_FAILED
          ? 'ERROR'
          : status;
  }

  // Apply aggregation logic: ANY failed → FAILED
  if (partitionStats.numFailed > 0 || partitionStats.numExecutionFailed > 0) {
    // Check severity to distinguish WARN from ERROR
    const severity = check.executionForLatestMaterialization?.evaluation?.severity;
    return severity === AssetCheckSeverity.WARN ? 'WARN' : 'ERROR';
  }

  if (partitionStats.numInProgress > 0) {
    return AssetCheckExecutionResolvedStatus.IN_PROGRESS;
  }

  if (partitionStats.numSkipped > 0) {
    return AssetCheckExecutionResolvedStatus.SKIPPED;
  }

  if (partitionStats.numSucceeded > 0) {
    return AssetCheckExecutionResolvedStatus.SUCCEEDED;
  }

  return 'NOT_EVALUATED';
}
