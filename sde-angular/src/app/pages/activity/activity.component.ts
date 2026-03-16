import { Component, inject, signal, computed, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { ApiService, ExecutionListItem, ExecutionProgress } from '../../services/api.service';
import { NgTemplateOutlet } from '@angular/common';
import { TenantService, Tenant } from '../../services/tenant.service';
import { ToastService } from '../../services/toast.service';

type TabValue = 'active' | 'history' | 'all';
type TypeFilter = 'All' | 'Extraction' | 'Ingestion';

function formatTime(iso?: string): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatDuration(ms?: number): string {
  if (!ms) return '';
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  return `${mins}m ${secs % 60}s`;
}

function getProgressPercentage(progress?: ExecutionProgress | null): number | null {
  if (!progress) return null;
  const rawPct =
    progress.percentage ??
    progress.progress ??
    (progress.totalSteps && progress.completedSteps
      ? (progress.completedSteps / progress.totalSteps) * 100
      : null);
  return typeof rawPct === 'number' && Number.isFinite(rawPct)
    ? Math.max(0, Math.min(100, Math.round(rawPct)))
    : null;
}

@Component({
  selector: 'app-job-row',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div
      class="bg-card border border-border rounded-lg p-4 space-y-3"
      [class.cursor-pointer]="!!job.failure"
      (click)="job.failure && toggleExpanded()"
    >
      <div class="flex items-start justify-between gap-3">
        <!-- Left: type + name -->
        <div class="flex items-center gap-3 min-w-0">
          <div
            class="h-9 w-9 rounded-lg flex items-center justify-center shrink-0 transition-transform duration-200 hover:scale-110"
            [class]="getTypeBg()"
          >
            @if (job?.transactionType === 'Ingestion') {
              <svg class="h-4 w-4 text-info" [class.animate-pulse]="job.status === 'RUNNING'" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
            } @else {
              <svg class="h-4 w-4 text-primary" [class.animate-pulse]="job?.status === 'RUNNING'" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 17V3"/><path d="m6 11 6 6 6-6"/><path d="M19 21H5"/></svg>
            }
          </div>
          <div class="min-w-0">
            <p class="text-sm font-semibold truncate">{{ job.name || job.executionId }}</p>
            <p class="text-xs text-muted-foreground capitalize">{{ job.transactionType }}</p>
          </div>
        </div>

        <!-- Right: status + actions -->
        <div class="flex items-center gap-2 shrink-0">
          <!-- Status badge -->
          <span
            class="inline-flex items-center gap-1 px-2 py-0.5 text-xs font-semibold rounded-full border"
            [class]="getStatusBadgeClass()"
          >
            @switch (job?.status) {
              @case ('RUNNING') { <svg class="h-3 w-3 mr-0.5 animate-spin" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg> }
              @case ('SUCCEEDED') { <svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg> }
              @case ('FAILED') { <svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg> }
              @case ('ABORTED') { <svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg> }
              @default { <svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> }
            }
            {{ getStatusLabel() }}
          </span>

          <!-- Cancel button -->
          @if (job.status === 'RUNNING' && onCancel) {
            <button
              (click)="onCancelClick($event)"
              [class.hidden]="confirmCancel()"
              class="flex items-center gap-1 px-2 h-7 text-xs text-destructive hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
            >
              <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><rect width="6" height="6" x="9" y="9"/></svg>
              Cancel
            </button>
            @if (confirmCancel()) {
              <div class="flex items-center gap-1">
                <button
                  (click)="doCancel($event)"
                  class="px-2 h-7 text-xs bg-destructive text-destructive-foreground rounded-md hover:bg-destructive/90 transition-colors"
                >Confirm</button>
                <button
                  (click)="confirmCancel.set(false); $event.stopPropagation()"
                  class="px-2 h-7 text-xs border border-border rounded-md hover:bg-secondary transition-colors"
                >Keep</button>
              </div>
            }
          }

          <!-- Rerun button -->
          @if (job.status !== 'RUNNING' && job.status !== 'PENDING_REDRIVE' && onRerun) {
            <button
              (click)="onRerunClick($event)"
              class="flex items-center gap-1 px-2 h-7 text-xs text-primary hover:text-primary hover:bg-primary/10 rounded-md transition-colors"
            >
              <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
              Rerun
            </button>
          }

          <!-- Expand chevron -->
          @if (job.failure) {
            <svg
              class="h-4 w-4 text-muted-foreground transition-transform"
              [class.rotate-180]="expanded()"
              xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
            ><path d="m6 9 6 6 6-6"/></svg>
          }
        </div>
      </div>

      <!-- Progress bar (running) -->
      @if (job.status === 'RUNNING') {
        <div class="space-y-1.5">
          <div class="flex items-center justify-between text-xs">
            <span class="text-muted-foreground">
              {{ job.progress?.currentStep || job.progress?.message || 'Processing…' }}
            </span>
            @if (progressPct() !== null) {
              <span class="font-medium tabular-nums">{{ progressPct() }}%</span>
            }
          </div>
          @if (progressPct() !== null) {
            <div class="relative h-2 w-full overflow-hidden rounded-full bg-secondary">
              <div class="h-full rounded-full bg-primary transition-all" [style.width]="progressPct() + '%'"></div>
            </div>
          } @else {
            <div class="relative h-2 w-full overflow-hidden rounded-full bg-secondary">
              <div class="h-full w-1/3 rounded-full bg-primary absolute" style="animation: shimmer 1.5s ease-in-out infinite;"></div>
            </div>
          }
          @if (job.progress?.totalSteps != null && job.progress?.completedSteps != null) {
            <p class="text-xs text-muted-foreground/70">
              Step {{ job.progress!.completedSteps }} of {{ job.progress!.totalSteps }}
            </p>
          }
        </div>
      }

      <!-- Expanded failure details -->
      @if (expanded() && job.failure) {
        <pre class="text-xs text-destructive bg-destructive/10 rounded px-3 py-2 overflow-x-auto whitespace-pre-wrap break-all max-h-60 overflow-y-auto">{{ job.failure.error }}{{ job.failure.cause ? '\n' + job.failure.cause : '' }}</pre>
      }

      <!-- Timestamps -->
      <div class="flex items-center gap-4 text-xs text-muted-foreground flex-wrap">
        <span>Started {{ formatTime(job.startDate) }}</span>
        @if (job.stopDate) { <span>• Ended {{ formatTime(job.stopDate) }}</span> }
        @if (job.durationMs != null) { <span>• {{ formatDuration(job.durationMs) }}</span> }
        @if (job.transactionType === 'Extraction' && job.recordsExtracted != null) {
          <span class="text-primary font-medium">• {{ job.recordsExtracted.toLocaleString() }} records extracted</span>
        }
        @if (job.transactionType === 'Ingestion' && job.recordsIngested != null) {
          <span class="text-info font-medium">• {{ job.recordsIngested.toLocaleString() }} records ingested</span>
        }
        <span class="font-mono text-muted-foreground/60">{{ job.executionId }}</span>
      </div>
    </div>
  `,
})
export class JobRowComponent {
  job!: ExecutionListItem;
  onCancel?: (executionId: string, executionArn: string) => void;
  onRerun?: (job: ExecutionListItem) => void;

  expanded = signal(false);
  confirmCancel = signal(false);

  formatTime = formatTime;
  formatDuration = formatDuration;

  progressPct = computed(() => getProgressPercentage(this.job?.progress));

  toggleExpanded(): void {
    this.expanded.update(v => !v);
  }

  onCancelClick(e: Event): void {
    e.stopPropagation();
    this.confirmCancel.set(true);
  }

  doCancel(e: Event): void {
    e.stopPropagation();
    this.onCancel?.(this.job.executionId, this.job.executionArn);
    this.confirmCancel.set(false);
  }

  onRerunClick(e: Event): void {
    e.stopPropagation();
    this.onRerun?.(this.job);
  }

  getStatusLabel(): string {
    const labels: Record<string, string> = {
      RUNNING: 'Running', SUCCEEDED: 'Succeeded', FAILED: 'Failed',
      TIMED_OUT: 'Timed Out', ABORTED: 'Aborted', PENDING_REDRIVE: 'Pending',
    };
    return labels[this.job?.status] ?? this.job?.status ?? '';
  }

  getStatusBadgeClass(): string {
    const classes: Record<string, string> = {
      RUNNING: 'text-info border-info/20',
      SUCCEEDED: 'text-success border-success/20',
      FAILED: 'text-destructive border-destructive/20',
      TIMED_OUT: 'text-warning border-warning/20',
      ABORTED: 'text-muted-foreground border-muted-foreground/20',
      PENDING_REDRIVE: 'text-warning border-warning/20',
    };
    return classes[this.job?.status] ?? 'text-muted-foreground border-border';
  }

  getStatusIconSvg(): string {
    const isRunning = this.job?.status === 'RUNNING';
    switch (this.job?.status) {
      case 'RUNNING':
        return `<svg class="h-3 w-3 mr-0.5 animate-spin" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;
      case 'SUCCEEDED':
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg>`;
      case 'FAILED':
      case 'ABORTED':
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg>`;
      case 'TIMED_OUT':
      case 'PENDING_REDRIVE':
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`;
      default:
        return '';
    }
  }

  getTypeBg(): string {
    return this.job?.transactionType === 'Ingestion' ? 'bg-info/10' : 'bg-primary/10';
  }

  getTypeIconSvg(): string {
    if (this.job?.transactionType === 'Ingestion') {
      return `<svg class="h-4 w-4 text-info${this.job.status === 'RUNNING' ? ' animate-pulse' : ''}" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>`;
    }
    return `<svg class="h-4 w-4 text-primary${this.job?.status === 'RUNNING' ? ' animate-pulse' : ''}" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 17V3"/><path d="m6 11 6 6 6-6"/><path d="M19 21H5"/></svg>`;
  }
}

@Component({
  selector: 'app-activity',
  standalone: true,
  imports: [CommonModule, NgTemplateOutlet],
  template: `
    <div class="space-y-8">
      <!-- Header with filters -->
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
            <svg class="h-5 w-5 text-primary" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>
          </div>
          <div>
            <h1 class="text-2xl font-bold">Activity</h1>
            <p class="text-sm text-muted-foreground">Monitor extraction and ingestion job progress</p>
          </div>
        </div>
        <div class="flex items-center gap-2">
          @for (t of typeFilters; track t) {
            <button
              (click)="setTypeFilter(t)"
              class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-md border transition-colors"
              [class]="typeFilter() === t ? 'bg-primary text-primary-foreground border-primary' : 'border-border text-foreground hover:bg-secondary'"
            >
              @if (t === 'Extraction') {
                <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 17V3"/><path d="m6 11 6 6 6-6"/><path d="M19 21H5"/></svg>
              }
              @if (t === 'Ingestion') {
                <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
              }
              {{ t }}
            </button>
          }
        </div>
      </div>

      <!-- Error -->
      @if (error()) {
        <div class="bg-destructive/10 border border-destructive/20 rounded-lg p-4 text-sm text-destructive">
          {{ error() }}
        </div>
      }

      <!-- Loading skeletons -->
      @if (loading()) {
        <div class="space-y-3">
          @for (i of [1,2,3]; track i) {
            <div class="h-24 w-full rounded-lg bg-secondary/50 animate-pulse"></div>
          }
        </div>
      } @else {
        <!-- Tabs -->
        <div>
          <div class="flex border-b border-border">
            @for (tab of tabs; track tab.value) {
              <button
                (click)="setActiveTab(tab.value)"
                class="px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px"
                [class]="activeTab() === tab.value ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'"
              >
                {{ tab.label }} ({{ getTabCount(tab.value) }})
              </button>
            }
          </div>

          <div class="mt-4 space-y-3">
            <!-- Active tab -->
            @if (activeTab() === 'active') {
              @if (running().length === 0) {
                <div class="text-center py-12 text-muted-foreground">
                  <svg class="h-8 w-8 mx-auto mb-2 text-success" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg>
                  <p class="text-sm">No active jobs</p>
                </div>
              } @else {
                @for (job of running(); track job.executionArn) {
                  <ng-container *ngTemplateOutlet="jobRowTpl; context: { job: job, showCancel: true, showRerun: false }"></ng-container>
                }
              }
            }

            <!-- History tab -->
            @if (activeTab() === 'history') {
              @if (history().length === 0) {
                <div class="text-center py-12 text-muted-foreground">
                  <p class="text-sm">No history yet</p>
                </div>
              } @else {
                @for (job of history(); track job.executionArn) {
                  <ng-container *ngTemplateOutlet="jobRowTpl; context: { job: job, showCancel: false, showRerun: true }"></ng-container>
                }
              }
            }

            <!-- All tab -->
            @if (activeTab() === 'all') {
              @if (statusFilter()) {
                <div class="flex items-center gap-2 mb-2">
                  <span class="inline-flex items-center px-2.5 py-0.5 text-xs font-semibold rounded-full bg-secondary text-secondary-foreground">
                    Showing: {{ statusFilter() === 'FAILED_ABORTED' ? 'Failed / Aborted' : statusFilter() }}
                  </span>
                  <button
                    (click)="statusFilter.set(null)"
                    class="px-2 h-6 text-xs hover:bg-secondary rounded-md transition-colors"
                  >Clear filter</button>
                </div>
              }
              @for (job of allFiltered(); track job.executionArn) {
                <ng-container *ngTemplateOutlet="jobRowTpl; context: { job: job, showCancel: job.status === 'RUNNING', showRerun: true }"></ng-container>
              }
            }
          </div>
        </div>
      }
    </div>

    <!-- Reusable job row template -->
    <ng-template #jobRowTpl let-job="job" let-showCancel="showCancel" let-showRerun="showRerun">
      <div
        class="bg-card border border-border rounded-lg p-4 space-y-3"
        [class.cursor-pointer]="!!job.failure"
        (click)="job.failure && toggleExpanded(job.executionArn)"
      >
        <div class="flex items-start justify-between gap-3">
          <div class="flex items-center gap-3 min-w-0">
            <div
              class="h-9 w-9 rounded-lg flex items-center justify-center shrink-0 transition-transform duration-200 hover:scale-110"
              [class]="job.transactionType === 'Ingestion' ? 'bg-info/10' : 'bg-primary/10'"
            >
              @if (job.transactionType === 'Ingestion') {
                <svg class="h-4 w-4 text-info" [class.animate-pulse]="job.status === 'RUNNING'" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
              } @else {
                <svg class="h-4 w-4 text-primary" [class.animate-pulse]="job.status === 'RUNNING'" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 17V3"/><path d="m6 11 6 6 6-6"/><path d="M19 21H5"/></svg>
              }
            </div>
            <div class="min-w-0">
              <p class="text-sm font-semibold truncate">{{ job.name || job.executionId }}</p>
              <p class="text-xs text-muted-foreground capitalize">{{ job.transactionType }}</p>
            </div>
          </div>

          <div class="flex items-center gap-2 shrink-0">
            <span class="inline-flex items-center gap-1 px-2 py-0.5 text-xs font-semibold rounded-full border" [class]="getStatusBadgeClass(job.status)">
              @switch (job.status) {
                @case ('RUNNING') { <svg class="h-3 w-3 animate-spin" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg> }
                @case ('SUCCEEDED') { <svg class="h-3 w-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg> }
                @case ('FAILED') { <svg class="h-3 w-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg> }
                @case ('ABORTED') { <svg class="h-3 w-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg> }
                @default { <svg class="h-3 w-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> }
              }
              {{ getStatusLabel(job.status) }}
            </span>

            @if (showCancel && job.status === 'RUNNING') {
              <button
                (click)="confirmAndCancel(job, $event)"
                class="flex items-center gap-1 px-2 h-7 text-xs text-destructive hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
              >
                <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><rect width="6" height="6" x="9" y="9"/></svg>
                Cancel
              </button>
            }
            @if (showRerun && job.status !== 'RUNNING' && job.status !== 'PENDING_REDRIVE') {
              <button
                (click)="handleRerun(job, $event)"
                class="flex items-center gap-1 px-2 h-7 text-xs text-primary hover:text-primary hover:bg-primary/10 rounded-md transition-colors"
              >
                <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
                Rerun
              </button>
            }

            @if (job.failure) {
              <svg class="h-4 w-4 text-muted-foreground transition-transform" [class.rotate-180]="isExpanded(job.executionArn)" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>
            }
          </div>
        </div>

        @if (job.status === 'RUNNING') {
          <div class="space-y-1.5">
            <div class="flex items-center justify-between text-xs">
              <span class="text-muted-foreground">{{ job.progress?.currentStep || job.progress?.message || job.progress?.status || 'Processing…' }}</span>
              @if (calcProgress(job.progress) !== null) {
                <span class="font-medium tabular-nums">{{ calcProgress(job.progress) }}%</span>
              }
            </div>
            @if (calcProgress(job.progress) !== null) {
              <div class="relative h-2 w-full overflow-hidden rounded-full bg-secondary">
                <div class="h-full rounded-full bg-primary transition-all" [style.width]="calcProgress(job.progress) + '%'"></div>
              </div>
            } @else {
              <div class="relative h-2 w-full overflow-hidden rounded-full bg-secondary">
                <div class="h-full w-1/3 rounded-full bg-primary absolute animate-shimmer"></div>
              </div>
            }
            @if (job.progress?.totalSteps != null && job.progress?.completedSteps != null) {
              <p class="text-xs text-muted-foreground/70">Step {{ job.progress!.completedSteps }} of {{ job.progress!.totalSteps }}</p>
            }
            @if (job.progress?.details?.['tables_done'] != null && job.progress?.details?.['total_tables'] != null) {
              <p class="text-xs text-muted-foreground/70">
                Table {{ job.progress!.details!['tables_done'] }} of {{ job.progress!.details!['total_tables'] }}
                @if (job.progress!.details!['current_table']) { — {{ job.progress!.details!['current_table'] }} }
              </p>
            }
          </div>
        }

        @if (isExpanded(job.executionArn) && job.failure) {
          <pre class="text-xs text-destructive bg-destructive/10 rounded px-3 py-2 overflow-x-auto whitespace-pre-wrap break-all max-h-60 overflow-y-auto">{{ job.failure.error }}{{ job.failure.cause ? '\n' + job.failure.cause : '' }}</pre>
        }

        <div class="flex items-center gap-4 text-xs text-muted-foreground flex-wrap">
          <span>Started {{ formatTime(job.startDate) }}</span>
          @if (job.stopDate) { <span>• Ended {{ formatTime(job.stopDate) }}</span> }
          @if (job.durationMs != null) { <span>• {{ formatDuration(job.durationMs) }}</span> }
          @if (job.transactionType === 'Extraction' && job.recordsExtracted != null) {
            <span class="text-primary font-medium">• {{ job.recordsExtracted.toLocaleString() }} records extracted</span>
          }
          @if (job.transactionType === 'Ingestion' && job.recordsIngested != null) {
            <span class="text-info font-medium">• {{ job.recordsIngested.toLocaleString() }} records ingested</span>
          }
          <span class="font-mono text-muted-foreground/60">{{ job.executionId }}</span>
        </div>
      </div>
    </ng-template>
  `,
})
export class ActivityComponent implements OnInit, OnDestroy {
  private readonly apiService = inject(ApiService);
  private readonly tenantService = inject(TenantService);
  private readonly toastService = inject(ToastService);
  private readonly router = inject(Router);
  private intervalId?: ReturnType<typeof setInterval>;

  typeFilters: TypeFilter[] = ['All', 'Extraction', 'Ingestion'];
  tabs = [
    { value: 'active' as TabValue, label: 'Active' },
    { value: 'history' as TabValue, label: 'History' },
    { value: 'all' as TabValue, label: 'All' },
  ];

  executions = signal<ExecutionListItem[]>([]);
  loading = signal(true);
  error = signal<string | null>(null);
  polling = signal(true);
  typeFilter = signal<TypeFilter>('All');
  statusFilter = signal<string | null>(null);
  activeTab = signal<TabValue>('active');
  expandedRows = signal<Set<string>>(new Set());

  filtered = computed(() => {
    const tf = this.typeFilter();
    return tf === 'All' ? this.executions() : this.executions().filter(e => e.transactionType === tf);
  });

  running = computed(() => this.filtered().filter(e => e.status === 'RUNNING' || e.status === 'PENDING_REDRIVE'));
  history = computed(() => this.filtered().filter(e => e.status !== 'RUNNING' && e.status !== 'PENDING_REDRIVE'));

  allFiltered = computed(() => {
    const sf = this.statusFilter();
    if (!sf) return this.filtered();
    return this.filtered().filter(e =>
      sf === 'FAILED_ABORTED' ? (e.status === 'FAILED' || e.status === 'ABORTED') : e.status === sf
    );
  });

  formatTime = formatTime;
  formatDuration = formatDuration;
  calcProgress = getProgressPercentage;

  ngOnInit(): void {
    const state = history.state as { tab?: string; typeFilter?: string; statusFilter?: string } | undefined;
    if (state?.typeFilter) this.typeFilter.set(state.typeFilter as TypeFilter);
    if (state?.statusFilter) this.statusFilter.set(state.statusFilter);
    if (state?.tab) this.activeTab.set(state.tab as TabValue);

    this.fetchExecutions();
    this.intervalId = setInterval(() => this.fetchExecutions(), 5000);
  }

  ngOnDestroy(): void {
    if (this.intervalId) clearInterval(this.intervalId);
  }

  private async fetchExecutions(): Promise<void> {
    try {
      const data = await this.apiService.listExecutions({
        maxResults: 50,
        tenant_id: this.tenantService.tenantId(),
        tenantResourceId: this.tenantService.tenantResourceId(),
      });

      this.executions.update(prev => {
        const prevByArn = new Map(prev.map(item => [item.executionArn, item]));
        const isActive = (s: string) => s === 'RUNNING' || s === 'PENDING_REDRIVE';

        return (data.items ?? []).map(item => {
          const previous = prevByArn.get(item.executionArn);
          if (!previous || !isActive(item.status)) return item;

          const nextPct = getProgressPercentage(item.progress);
          const prevPct = getProgressPercentage(previous.progress);

          if (prevPct != null && nextPct != null && nextPct < prevPct) {
            return { ...item, progress: previous.progress };
          }
          if (item.progress && previous.progress) {
            return { ...item, progress: { ...previous.progress, ...item.progress } };
          }
          if (!item.progress && previous.progress) {
            return { ...item, progress: previous.progress };
          }
          return item;
        });
      });
      this.error.set(null);
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Failed to load executions');
    } finally {
      this.loading.set(false);
    }
  }

  async confirmAndCancel(job: ExecutionListItem, e: Event): Promise<void> {
    e.stopPropagation();
    if (!confirm(`Cancel execution "${job.name || job.executionId}"? This cannot be undone.`)) return;
    try {
      await this.apiService.cancelExecution(job.executionId, job.executionArn);
      this.toastService.toast({ title: 'Execution cancelled', description: 'The job has been aborted.' });
      await this.fetchExecutions();
    } catch (err) {
      this.toastService.toast({ title: 'Failed to cancel', description: err instanceof Error ? err.message : 'Unknown error', variant: 'destructive' });
    }
  }

  async handleRerun(job: ExecutionListItem, e: Event): Promise<void> {
    e.stopPropagation();
    try {
      const detail = await this.apiService.getExecutionStatus(job.executionId, job.executionArn);
      const input = detail.input as Record<string, unknown> | undefined;

      const tenantFromInput = typeof input?.['tenant_id'] === 'string' ? input['tenant_id'] : '';
      if (tenantFromInput) {
        const matchedTenant = this.tenantService.tenants.find((t: Tenant) => t.tenantId === tenantFromInput);
        if (matchedTenant) this.tenantService.setCurrentTenant(matchedTenant);
      }

      if (job.transactionType === 'Extraction') {
        const rawExposureIds = input?.['exposureSetIds'] ?? input?.['exposure_set_ids'] ?? input?.['exposure_ids'];
        const exposureSetIds = Array.isArray(rawExposureIds)
          ? rawExposureIds.map(v => Number(v)).filter(v => Number.isFinite(v))
          : [];

        const s3OutputPath =
          (typeof input?.['s3OutputPath'] === 'string' && input['s3OutputPath']) ||
          (typeof input?.['s3_output_path'] === 'string' && input['s3_output_path']) || '';

        const exportType =
          (typeof input?.['exportType'] === 'string' && input['exportType']) ||
          (typeof input?.['export_type'] === 'string' && input['export_type']) || 'mdf';

        this.router.navigate(['/extraction'], {
          state: {
            rerun: true, s3OutputPath, exposureSetIds, exportType,
            executionName: `rerun-${job.name || job.executionId}-${Math.floor(Math.random() * 10000)}`,
            tenant_id: tenantFromInput,
          },
        });
      } else if (job.transactionType === 'Ingestion') {
        const fileName =
          (typeof input?.['fileName'] === 'string' && input['fileName']) ||
          (typeof input?.['file_name'] === 'string' && input['file_name']) || '';
        const fileNameOnly = fileName.split('/').pop() || '';
        this.router.navigate(['/ingestion'], {
          state: {
            rerun: true, fileName: fileNameOnly,
            executionName: `rerun-${job.name || job.executionId}-${Math.floor(Math.random() * 10000)}`,
          },
        });
      } else {
        this.toastService.toast({ title: 'Rerun not supported', description: 'Rerun is not supported for this job type.' });
      }
    } catch (err) {
      this.toastService.toast({ title: 'Failed to load job details', description: err instanceof Error ? err.message : 'Unknown error', variant: 'destructive' });
    }
  }

  setTypeFilter(f: TypeFilter): void {
    this.typeFilter.set(f);
    this.statusFilter.set(null);
  }

  setActiveTab(tab: TabValue): void {
    this.activeTab.set(tab);
    this.statusFilter.set(null);
  }

  toggleExpanded(arn: string): void {
    this.expandedRows.update(s => {
      const next = new Set(s);
      next.has(arn) ? next.delete(arn) : next.add(arn);
      return next;
    });
  }

  isExpanded(arn: string): boolean {
    return this.expandedRows().has(arn);
  }

  getTabCount(tab: TabValue): number {
    switch (tab) {
      case 'active': return this.running().length;
      case 'history': return this.history().length;
      case 'all': return this.filtered().length;
    }
  }

  getStatusLabel(status: string): string {
    const labels: Record<string, string> = {
      RUNNING: 'Running', SUCCEEDED: 'Succeeded', FAILED: 'Failed',
      TIMED_OUT: 'Timed Out', ABORTED: 'Aborted', PENDING_REDRIVE: 'Pending',
    };
    return labels[status] ?? status;
  }

  getStatusBadgeClass(status: string): string {
    const classes: Record<string, string> = {
      RUNNING: 'text-info border-info/20',
      SUCCEEDED: 'text-success border-success/20',
      FAILED: 'text-destructive border-destructive/20',
      TIMED_OUT: 'text-warning border-warning/20',
      ABORTED: 'text-muted-foreground border-muted-foreground/20',
      PENDING_REDRIVE: 'text-warning border-warning/20',
    };
    return classes[status] ?? 'text-muted-foreground border-border';
  }

  getStatusIconSvg(status: string): string {
    switch (status) {
      case 'RUNNING':
        return `<svg class="h-3 w-3 mr-0.5 animate-spin" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;
      case 'SUCCEEDED':
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg>`;
      case 'FAILED': case 'ABORTED':
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg>`;
      default:
        return `<svg class="h-3 w-3 mr-0.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`;
    }
  }
}
