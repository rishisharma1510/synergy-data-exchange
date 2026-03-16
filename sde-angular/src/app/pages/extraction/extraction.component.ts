import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { TenantService } from '../../services/tenant.service';
import { ToastService } from '../../services/toast.service';
import { SynergyDrivePickerComponent, S3Item } from '../../components/synergy-drive-picker/synergy-drive-picker.component';

type ExportType = 'mdf' | 'bak' | 'csv' | 'clf';

@Component({
  selector: 'app-extraction',
  standalone: true,
  imports: [CommonModule, FormsModule, SynergyDrivePickerComponent],
  template: `
    <div class="space-y-8">
      <!-- Header -->
      <div>
        <div class="flex items-center gap-3">
          <div class="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
            <svg class="h-5 w-5 text-primary" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
          </div>
          <div>
            <h1 class="text-2xl font-bold">Data Extraction</h1>
            <p class="text-sm text-muted-foreground">Export from Iceberg Glue database to SQL Server file</p>
          </div>
        </div>
      </div>

      <!-- Step 1: Output Folder -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold">1. Output Folder</h2>
          <p class="text-sm text-muted-foreground">Select a destination folder on Synergy Drive</p>
        </div>
        <app-synergy-drive-picker
          mode="folder"
          [selectedKey]="selectedFolder()?.key"
          (onSelect)="onFolderSelect($event)"
        />
        <p class="text-sm text-primary font-mono">📁 {{ outputPath() }}</p>
      </section>

      <!-- Step 2: Execution Name -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold">2. Execution Name</h2>
          <p class="text-sm text-muted-foreground">A unique name for this extraction run</p>
        </div>
        <input
          [(ngModel)]="executionName"
          placeholder="e.g. my-extraction-run"
          class="w-full h-9 px-3 rounded-md border border-input bg-background text-sm focus:outline-none focus:ring-1 focus:ring-ring"
        />
      </section>

      <!-- Step 3: Export Type -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold">3. Export Type</h2>
          <p class="text-sm text-muted-foreground">Select the output file format</p>
        </div>
        <select
          [(ngModel)]="exportType"
          class="w-full h-9 px-3 rounded-md border border-input bg-background text-sm focus:outline-none focus:ring-1 focus:ring-ring"
        >
          <option value="mdf">MDF</option>
          <option value="bak">BAK</option>
          <option value="csv">CSV</option>
          <option value="clf">CLF</option>
        </select>
      </section>

      <!-- Step 4: ExposureSetSIDs -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold">4. ExposureSetSIDs</h2>
          <p class="text-sm text-muted-foreground">Enter comma-separated numeric IDs</p>
        </div>
        <input
          [(ngModel)]="exposureSids"
          placeholder="e.g. 1, 2, 3, 4"
          class="w-full h-9 px-3 rounded-md border border-input bg-background text-sm focus:outline-none focus:ring-1 focus:ring-ring"
        />
      </section>

      <!-- Submit -->
      <div class="flex justify-end">
        <button
          (click)="handleSubmit()"
          [disabled]="isSubmitting() || !exposureSids.trim()"
          class="flex items-center gap-2 px-6 py-3 rounded-md text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors glow-primary"
        >
          <!-- Rocket icon -->
          <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/></svg>
          {{ isSubmitting() ? 'Triggering...' : 'Start Extraction' }}
        </button>
      </div>
    </div>
  `,
})
export class ExtractionComponent implements OnInit {
  private readonly apiService = inject(ApiService);
  private readonly tenantService = inject(TenantService);
  private readonly toastService = inject(ToastService);
  readonly router = inject(Router);

  selectedFolder = signal<S3Item | null>(null);
  outputPath = signal('/Synergy-Drive/Outbox/databridgetest/');
  executionName = `activity-extraction-${Math.floor(Math.random() * 100000)}`;
  exposureSids = '';
  exportType: ExportType = 'mdf';
  isSubmitting = signal(false);

  ngOnInit(): void {
    const state = history.state as { rerun?: boolean; s3OutputPath?: string; exposureSetIds?: number[]; exportType?: ExportType; executionName?: string } | undefined;
    if (state?.rerun) {
      if (state.executionName) this.executionName = state.executionName;
      if (state.exposureSetIds) this.exposureSids = state.exposureSetIds.join(', ');
      if (state.exportType) this.exportType = state.exportType;
      if (state.s3OutputPath) this.outputPath.set(state.s3OutputPath);
    }
  }

  onFolderSelect(item: S3Item): void {
    this.selectedFolder.set(item);
    this.outputPath.set(`/Synergy-Drive/${item.key}`);
  }

  async handleSubmit(): Promise<void> {
    const parsed = this.exposureSids
      .split(',')
      .map(s => s.trim())
      .filter(Boolean)
      .map(Number);

    if (parsed.length === 0 || parsed.some(isNaN)) {
      this.toastService.error('Please enter valid comma-separated numbers for ExposureSetSIDs');
      return;
    }
    if (!this.executionName.trim()) {
      this.toastService.error('Please enter an execution name');
      return;
    }
    if (this.exportType === 'csv' || this.exportType === 'clf') {
      this.toastService.info(`${this.exportType.toUpperCase()} export is coming soon!`, 'This format is not yet supported. Please select MDF or BAK for now.');
      return;
    }

    this.isSubmitting.set(true);
    try {
      const response = await this.apiService.startExtraction({
        transactionType: 'Extraction',
        exposureSetIds: parsed,
        s3OutputPath: this.outputPath(),
        executionName: this.executionName.trim(),
        tenant_id: this.tenantService.tenantId(),
        tenantResourceId: this.tenantService.tenantResourceId(),
        exportType: this.exportType,
      });
      this.toastService.success('Extraction job triggered!', `Execution: ${response.executionId}`);
      this.router.navigate(['/activity']);
    } catch (err) {
      this.toastService.error('Failed to trigger extraction job', err instanceof Error ? err.message : 'Unknown error');
    } finally {
      this.isSubmitting.set(false);
    }
  }
}
