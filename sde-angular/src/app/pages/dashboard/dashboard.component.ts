import { Component, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { TenantService } from '../../services/tenant.service';

interface StatCard {
  label: string;
  value: string;
  icon: string;
  color: string;
}

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="space-y-8">
      <div>
        <p class="text-muted-foreground">Manage your data extraction and ingestion workflows</p>
      </div>

      <!-- Stats grid -->
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        @for (stat of stats(); track stat.label) {
          <div
            (click)="handleStatClick(stat.label)"
            class="group relative bg-card border border-border rounded-xl p-5 cursor-pointer transition-all duration-300 hover:border-primary/40 hover:shadow-[0_0_20px_-5px_hsl(var(--primary)/0.25)] hover:-translate-y-0.5"
          >
            <div class="flex items-center justify-between">
              <span class="text-sm text-muted-foreground group-hover:text-foreground transition-colors">{{ stat.label }}</span>
              <div class="h-9 w-9 rounded-lg bg-secondary/60 flex items-center justify-center group-hover:bg-secondary transition-colors">
                <span class="flex items-center justify-center transition-transform group-hover:scale-110">
                  @switch (stat.icon) {
                    @case ('database') {
                      <svg class="h-5 w-5 text-primary" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                    }
                    @case ('upload') {
                      <svg class="h-5 w-5 text-info" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
                    }
                    @case ('activity') {
                      <svg class="h-5 w-5 text-warning" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>
                    }
                    @case ('check-circle') {
                      <svg class="h-5 w-5 text-success" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/></svg>
                    }
                    @case ('alert-triangle') {
                      <svg class="h-5 w-5 text-destructive" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>
                    }
                  }
                </span>
              </div>
            </div>
            <p class="text-3xl font-bold mt-3 tracking-tight">{{ stat.value }}</p>
          </div>
        }
      </div>

      <!-- Quick actions -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <button
          (click)="router.navigate(['/extraction'])"
          class="group bg-card border border-border rounded-lg p-6 text-left hover:border-primary/50 transition-colors glow-primary"
        >
          <svg class="h-8 w-8 text-primary mb-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
          <h2 class="text-lg font-semibold mb-1">Data Extraction</h2>
          <p class="text-sm text-muted-foreground">
            Export data from Iceberg Glue database to SQL Server MDF or BAK files and upload to S3.
          </p>
        </button>

        <button
          (click)="router.navigate(['/ingestion'])"
          class="group bg-card border border-border rounded-lg p-6 text-left hover:border-info/50 transition-colors"
        >
          <svg class="h-8 w-8 text-info mb-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
          <h2 class="text-lg font-semibold mb-1">Data Ingestion</h2>
          <p class="text-sm text-muted-foreground">
            Import MDF or BAK files from S3 with optional column mapping to your destination database.
          </p>
        </button>
      </div>
    </div>
  `,
})
export class DashboardComponent implements OnInit, OnDestroy {
  readonly router = inject(Router);
  private readonly apiService = inject(ApiService);
  private readonly tenantService = inject(TenantService);
  private intervalId?: ReturnType<typeof setInterval>;

  stats = signal<StatCard[]>([
    { label: 'Extractions',     value: '—', icon: 'database',        color: 'text-primary' },
    { label: 'Ingestions',      value: '—', icon: 'upload',          color: 'text-info' },
    { label: 'Running',         value: '—', icon: 'activity',        color: 'text-warning' },
    { label: 'Succeeded',       value: '—', icon: 'check-circle',    color: 'text-success' },
    { label: 'Failed / Aborted',value: '—', icon: 'alert-triangle',  color: 'text-destructive' },
  ]);

  ngOnInit(): void {
    this.fetchStats();
    this.intervalId = setInterval(() => this.fetchStats(), 10000);
  }

  ngOnDestroy(): void {
    if (this.intervalId) clearInterval(this.intervalId);
  }

  private async fetchStats(): Promise<void> {
    try {
      const data = await this.apiService.listExecutions({
        maxResults: 100,
        tenant_id: this.tenantService.tenantId(),
      });
      const items = data.items ?? [];
      const extractionCount = data.extractionCount ?? items.filter(i => i.transactionType === 'Extraction').length;
      const ingestionCount  = data.ingestionCount  ?? items.filter(i => i.transactionType === 'Ingestion').length;
      const runningCount    = data.runningCount    ?? items.filter(i => i.status === 'RUNNING').length;
      const succeededCount  = data.succeededCount  ?? items.filter(i => i.status === 'SUCCEEDED').length;
      const failedCount     = (data.failedCount    ?? items.filter(i => i.status === 'FAILED').length)
                            + (data.abortedCount   ?? items.filter(i => i.status === 'ABORTED').length);

      this.stats.set([
        { label: 'Extractions',      value: String(extractionCount), icon: 'database',       color: 'text-primary' },
        { label: 'Ingestions',       value: String(ingestionCount),  icon: 'upload',         color: 'text-info' },
        { label: 'Running',          value: String(runningCount),    icon: 'activity',       color: 'text-warning' },
        { label: 'Succeeded',        value: String(succeededCount),  icon: 'check-circle',   color: 'text-success' },
        { label: 'Failed / Aborted', value: String(failedCount),     icon: 'alert-triangle', color: 'text-destructive' },
      ]);
    } catch {}
  }

  handleStatClick(label: string): void {
    switch (label) {
      case 'Extractions':
        this.router.navigate(['/activity'], { state: { tab: 'history', typeFilter: 'Extraction' } });
        break;
      case 'Ingestions':
        this.router.navigate(['/activity'], { state: { tab: 'history', typeFilter: 'Ingestion' } });
        break;
      case 'Running':
        this.router.navigate(['/activity'], { state: { tab: 'active' } });
        break;
      case 'Succeeded':
        this.router.navigate(['/activity'], { state: { tab: 'all', statusFilter: 'SUCCEEDED' } });
        break;
      case 'Failed / Aborted':
        this.router.navigate(['/activity'], { state: { tab: 'all', statusFilter: 'FAILED_ABORTED' } });
        break;
    }
  }
}
