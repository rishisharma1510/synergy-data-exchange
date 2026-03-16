import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

export interface ColumnMapping {
  id: string;
  source: string;
  destination: string;
}

@Component({
  selector: 'app-column-mapper',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <div>
          <h3 class="text-sm font-semibold">Column Mapping</h3>
          <p class="text-xs text-muted-foreground">Optional — map source columns to destination columns</p>
        </div>
        <button
          (click)="addMapping()"
          class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border border-border rounded-md hover:bg-secondary transition-colors"
        >
          <!-- Plus icon -->
          <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="M12 5v14"/></svg>
          Add Mapping
        </button>
      </div>

      @if (mappings.length > 0) {
        <div class="border border-border rounded-lg overflow-hidden">
          <div class="grid grid-cols-[1fr_auto_1fr_auto] gap-2 px-4 py-2 bg-secondary/50 text-xs font-medium text-muted-foreground">
            <span>Source Column</span>
            <span></span>
            <span>Destination Column</span>
            <span></span>
          </div>
          @for (mapping of mappings; track mapping.id) {
            <div class="grid grid-cols-[1fr_auto_1fr_auto] gap-2 items-center px-4 py-2 border-t border-border">
              <input
                placeholder="source_column"
                [(ngModel)]="mapping.source"
                (ngModelChange)="emitChange()"
                class="h-8 text-sm font-mono px-3 rounded-md border border-input bg-background focus:outline-none focus:ring-1 focus:ring-ring"
              />
              <!-- ArrowRight -->
              <svg class="h-4 w-4 text-muted-foreground" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg>
              <input
                placeholder="destination_column"
                [(ngModel)]="mapping.destination"
                (ngModelChange)="emitChange()"
                class="h-8 text-sm font-mono px-3 rounded-md border border-input bg-background focus:outline-none focus:ring-1 focus:ring-ring"
              />
              <button
                (click)="removeMapping(mapping.id)"
                class="h-8 w-8 flex items-center justify-center rounded-md hover:bg-destructive/10 transition-colors"
              >
                <!-- Trash2 -->
                <svg class="h-3.5 w-3.5 text-destructive" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>
              </button>
            </div>
          }
        </div>
      }

      @if (mappings.length === 0) {
        <div class="border border-dashed border-border rounded-lg p-6 text-center text-sm text-muted-foreground">
          No mappings defined. Columns will be mapped by name automatically.
        </div>
      }
    </div>
  `,
})
export class ColumnMapperComponent {
  @Input() mappings: ColumnMapping[] = [];
  @Output() mappingsChange = new EventEmitter<ColumnMapping[]>();

  addMapping(): void {
    this.mappings = [...this.mappings, { id: crypto.randomUUID(), source: '', destination: '' }];
    this.mappingsChange.emit(this.mappings);
  }

  removeMapping(id: string): void {
    this.mappings = this.mappings.filter(m => m.id !== id);
    this.mappingsChange.emit(this.mappings);
  }

  emitChange(): void {
    this.mappingsChange.emit([...this.mappings]);
  }
}
