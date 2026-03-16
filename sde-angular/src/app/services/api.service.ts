import { Injectable } from '@angular/core';

const BASE_URL = 'https://gqgg2d599g.execute-api.us-east-1.amazonaws.com/api/api';

// ── Types ──────────────────────────────────────────────

export type TransactionType = 'Extraction' | 'Ingestion';

export interface ExecutionFailure {
  error?: string;
  cause?: string;
  failedAt?: string;
}

export interface ExecutionProgress {
  percentage?: number;
  progress?: number;
  status?: string;
  currentStep?: string;
  totalSteps?: number;
  completedSteps?: number;
  message?: string;
  details?: Record<string, unknown>;
}

export interface ExecutionListItem {
  executionArn: string;
  executionId: string;
  name?: string;
  transactionType: string;
  tenantResourceId?: string | null;
  status: string;
  isComplete: boolean;
  startDate?: string;
  stopDate?: string;
  durationMs?: number;
  failure?: ExecutionFailure;
  detailUrl?: string;
  progress?: ExecutionProgress | null;
  recordsExtracted?: number | null;
  recordsIngested?: number | null;
}

export interface StatusBreakdown {
  running: number;
  succeeded: number;
  failed: number;
  timedOut: number;
  aborted: number;
  total: number;
}

export interface ListExecutionsResponse {
  items: ExecutionListItem[];
  totalCount: number;
  extractionCount: number;
  ingestionCount: number;
  runningCount: number;
  succeededCount: number;
  failedCount: number;
  timedOutCount: number;
  abortedCount: number;
  extractionBreakdown?: StatusBreakdown;
  ingestionBreakdown?: StatusBreakdown;
  isTruncated?: boolean;
  appliedFilters?: Record<string, string | null>;
}

export interface ExecutionEvent {
  id: number;
  previousEventId: number;
  timestamp: string;
  type?: string;
  stateName?: string;
  details?: Record<string, string>;
}

export interface ExecutionStatusResponse {
  executionArn: string;
  executionId: string;
  name?: string;
  transactionType?: string;
  status: string;
  isComplete: boolean;
  startDate?: string;
  stopDate?: string;
  durationMs?: number;
  input?: unknown;
  output?: unknown;
  failure?: ExecutionFailure;
  events?: ExecutionEvent[];
}

export interface StartExecutionRequest {
  transactionType: TransactionType;
  executionName?: string;
  s3OutputPath?: string;
  fileName?: string;
  input?: unknown;
  metadata?: Record<string, string>;
}

export interface StartExecutionResponse {
  transactionType: string;
  executionArn: string;
  executionId: string;
  startDate: string;
  statusUrl: string;
}

export interface StartExtractionRequest {
  transactionType: 'Extraction';
  exposureSetIds: number[];
  s3OutputPath: string;
  executionName: string;
  tenant_id: string;
  tenantResourceId?: string;
  exportType: 'mdf' | 'bak' | 'csv' | 'clf';
}

// ── API Service ────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class ApiService {

  private async apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${BASE_URL}${path}`, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers ?? {}),
      },
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`API ${res.status}: ${body}`);
    }
    return res.json() as Promise<T>;
  }

  listExecutions(params?: {
    transactionType?: TransactionType;
    status?: string;
    maxResults?: number;
    tenant_id?: string;
    tenantResourceId?: string;
  }): Promise<ListExecutionsResponse> {
    const qs = new URLSearchParams();
    if (params?.transactionType) qs.set('transactionType', params.transactionType);
    if (params?.status) qs.set('status', params.status);
    if (params?.maxResults) qs.set('maxResults', String(params.maxResults));
    if (params?.tenant_id) qs.set('tenant_id', params.tenant_id);
    if (params?.tenantResourceId) qs.set('tenantResourceId', params.tenantResourceId);
    const query = qs.toString();
    return this.apiFetch<ListExecutionsResponse>(`/executions${query ? `?${query}` : ''}`);
  }

  startExecution(body: StartExecutionRequest): Promise<StartExecutionResponse> {
    return this.apiFetch<StartExecutionResponse>('/executions', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  startExtraction(body: StartExtractionRequest): Promise<StartExecutionResponse> {
    return this.apiFetch<StartExecutionResponse>('/executions', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  getExecutionStatus(executionId: string, executionArn: string): Promise<ExecutionStatusResponse> {
    return this.apiFetch<ExecutionStatusResponse>(
      `/executions/${encodeURIComponent(executionId)}?executionArn=${encodeURIComponent(executionArn)}`,
    );
  }

  cancelExecution(executionId: string, executionArn: string): Promise<void> {
    return this.apiFetch<void>(
      `/executions/${encodeURIComponent(executionId)}?executionArn=${encodeURIComponent(executionArn)}`,
      { method: 'DELETE' },
    );
  }
}
