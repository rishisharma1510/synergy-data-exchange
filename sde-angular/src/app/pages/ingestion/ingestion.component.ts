import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { TenantService } from '../../services/tenant.service';
import { ToastService } from '../../services/toast.service';
import { SynergyDrivePickerComponent, S3Item } from '../../components/synergy-drive-picker/synergy-drive-picker.component';
import { ColumnMapperComponent, ColumnMapping } from '../../components/column-mapper/column-mapper.component';

@Component({
  selector: 'app-ingestion',
  standalone: true,
  imports: [CommonModule, FormsModule, SynergyDrivePickerComponent, ColumnMapperComponent],
  template: `
    <div class="space-y-8">
      <!-- Header -->
      <div>
        <div class="flex items-center gap-3">
          <div class="h-10 w-10 rounded-lg bg-info/10 flex items-center justify-center">
            <svg class="h-5 w-5 text-info" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
          </div>
          <div>
            <h1 class="text-2xl font-bold">Data Ingestion</h1>
            <p class="text-sm text-muted-foreground">Import MDF or BAK files into your destination database</p>
          </div>
        </div>
      </div>

      <!-- Step 1: Source File -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold">1. Select Source File</h2>
          <p class="text-sm text-muted-foreground">Choose an MDF or BAK file from Synergy Drive</p>
        </div>
        <app-synergy-drive-picker
          mode="file"
          [fileFilter]="['.mdf', '.bak']"
          [selectedKey]="selectedFile()?.key"
          (onSelect)="selectedFile.set($event)"
        />
        @if (selectedFile()) {
          <p class="text-sm text-info font-mono">📄 {{ selectedFile()!.name }}</p>
        }
      </section>

      <!-- Step 2: Column Mapping -->
      <section class="bg-card border border-border rounded-lg p-6 space-y-4">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-base font-semibold">2. Column Mapping</h2>
            <p class="text-sm text-muted-foreground">Optionally define how source columns map to destination</p>
          </div>
          <div class="flex items-center gap-2">
            <button
              (click)="toggleMapping()"
              class="relative inline-flex h-5 w-9 cursor-pointer rounded-full border-2 border-transparent transition-colors"
              [class.bg-primary]="enableMapping()"
              [class.bg-secondary]="!enableMapping()"
              role="switch"
              [attr.aria-checked]="enableMapping()"
            >
              <span
                class="pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-lg ring-0 transition duration-200 ease-in-out"
                [class.translate-x-4]="enableMapping()"
                [class.translate-x-0]="!enableMapping()"
              ></span>
            </button>
            <label class="text-sm">Enable</label>
          </div>
        </div>
        @if (enableMapping()) {
          <app-column-mapper
            [mappings]="mappings()"
            (mappingsChange)="mappings.set($event)"
          />
        }
      </section>

      <!-- Submit -->
      <div class="flex justify-end">
        <button
          (click)="handleSubmit()"
          [disabled]="!selectedFile() || isSubmitting()"
          class="flex items-center gap-2 px-6 py-3 rounded-md text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <!-- Rocket icon -->
          <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/></svg>
          {{ isSubmitting() ? 'Triggering...' : 'Start Ingestion' }}
        </button>
      </div>
    </div>
  `,
})
export class IngestionComponent implements OnInit {
  private readonly apiService = inject(ApiService);
  private readonly tenantService = inject(TenantService);
  private readonly toastService = inject(ToastService);
  readonly router = inject(Router);

  selectedFile = signal<S3Item | null>(null);
  enableMapping = signal(false);
  mappings = signal<ColumnMapping[]>([]);
  isSubmitting = signal(false);

  ngOnInit(): void {
    const state = history.state as { rerun?: boolean; fileName?: string } | undefined;
    if (state?.rerun && state.fileName) {
      this.selectedFile.set({
        key: `IngestionInput/${state.fileName}`,
        name: state.fileName,
        type: 'file',
      });
    }
  }

  toggleMapping(): void {
    this.enableMapping.update(v => !v);
  }

  async handleSubmit(): Promise<void> {
    const file = this.selectedFile();
    if (!file) {
      this.toastService.error('Please select a source file');
      return;
    }

    this.isSubmitting.set(true);
    try {
      const fileNameWithoutExt = file.name.replace(/\.[^/.]+$/, '');
      const randomSuffix = Math.floor(Math.random() * 100000);
      const executionName = `${fileNameWithoutExt}_${randomSuffix}`;

      const response = await this.apiService.startExecution({
        transactionType: 'Ingestion',
        fileName: `/Synergy-Drive/IngestionInput/${file.name}`,
        executionName,
        metadata: {
          tenant_id: this.tenantService.tenantId(),
          tenantResourceId: this.tenantService.tenantResourceId(),
        },
      });
      this.toastService.success('Ingestion job triggered!', `Execution: ${response.executionId}`);
      this.router.navigate(['/activity']);
    } catch (err) {
      this.toastService.error('Failed to trigger ingestion job', err instanceof Error ? err.message : 'Unknown error');
    } finally {
      this.isSubmitting.set(false);
    }
  }
}
