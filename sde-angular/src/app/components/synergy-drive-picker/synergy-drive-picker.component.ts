import { Component, Input, Output, EventEmitter, signal } from '@angular/core';
import { CommonModule } from '@angular/common';

export interface S3Item {
  key: string;
  name: string;
  type: 'folder' | 'file';
  size?: number;
  lastModified?: string;
  children?: S3Item[];
}

const MOCK_TREE: S3Item[] = [
  {
    key: 'Inbox/', name: 'Inbox', type: 'folder', children: [
      {
        key: 'Inbox/Q1_Reports/', name: 'Q1_Reports', type: 'folder', children: [
          { key: 'Inbox/Q1_Reports/summary.bak', name: 'summary.bak', type: 'file', size: 134217728 },
        ]
      },
      {
        key: 'Inbox/Client_Uploads/', name: 'Client_Uploads', type: 'folder', children: [
          { key: 'Inbox/Client_Uploads/policy_data.mdf', name: 'policy_data.mdf', type: 'file', size: 268435456 },
        ]
      },
      { key: 'Inbox/Staging/', name: 'Staging', type: 'folder', children: [] },
    ]
  },
  {
    key: 'Outbox/', name: 'Outbox', type: 'folder', children: [
      { key: 'Outbox/Argenta_ExpDB.mdf', name: 'Argenta_ExpDB.mdf', type: 'file', size: 524288000 },
      { key: 'Outbox/Amtrust_Exp_TS6_UKN.mdf', name: 'Amtrust_Exp_TS6_UKN.mdf', type: 'file', size: 734003200 },
      { key: 'Outbox/Caribbean_v7_CEDE.mdf', name: 'Caribbean_v7_CEDE.mdf', type: 'file', size: 419430400 },
      { key: 'Outbox/AIRExposure_USAA_Property.mdf', name: 'AIRExposure_USAA_Property.mdf', type: 'file', size: 1073741824 },
    ]
  },
];

function formatSize(bytes?: number): string {
  if (!bytes) return '';
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return `${gb.toFixed(1)} GB`;
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(0)} MB`;
}

@Component({
  selector: 'app-tree-node',
  standalone: true,
  imports: [CommonModule],
  template: `
    @if (!(mode === 'folder' && item.type === 'file')) {
      <div>
        <button
          (click)="handleClick()"
          class="flex items-center gap-2 w-full px-2 py-1.5 text-sm rounded-md transition-colors"
          [class]="getButtonClass()"
          [style.padding-left]="(depth * 20 + 8) + 'px'"
        >
          @if (item.type === 'folder') {
            @if (expanded()) {
              <!-- ChevronDown -->
              <svg class="h-3.5 w-3.5 shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>
            } @else {
              <!-- ChevronRight -->
              <svg class="h-3.5 w-3.5 shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>
            }
            <!-- Folder icon -->
            <svg class="h-4 w-4 shrink-0 text-warning" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20h16a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.93a2 2 0 0 1-1.66-.9l-.82-1.2A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13c0 1.1.9 2 2 2Z"/></svg>
          } @else {
            <span class="w-3.5"></span>
            <!-- File icon -->
            <svg class="h-4 w-4 shrink-0 text-muted-foreground" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><polyline points="14 2 14 8 20 8"/></svg>
          }
          <span class="truncate">{{ item.name }}</span>
          @if (item.size) {
            <span class="ml-auto text-xs text-muted-foreground">{{ formatSize(item.size) }}</span>
          }
        </button>
        @if (expanded() && item.children) {
          @for (child of item.children; track child.key) {
            <app-tree-node
              [item]="child"
              [depth]="depth + 1"
              [mode]="mode"
              [fileFilter]="fileFilter"
              [selectedKey]="selectedKey"
              (selected)="onChildSelected($event)"
            />
          }
        }
      </div>
    }
  `,
})
export class TreeNodeComponent {
  @Input() item!: S3Item;
  @Input() depth = 0;
  @Input() mode: 'file' | 'folder' = 'file';
  @Input() fileFilter?: string[];
  @Input() selectedKey?: string;
  @Output() selected = new EventEmitter<S3Item>();

  expanded = signal(false);

  ngOnInit(): void {
    this.expanded.set(this.depth === 0);
  }

  formatSize = formatSize;

  isSelectable(): boolean {
    return this.mode === 'folder'
      ? this.item.type === 'folder'
      : this.item.type === 'file' && (!this.fileFilter || this.fileFilter.some(ext => this.item.name.endsWith(ext)));
  }

  isSelected(): boolean {
    return this.item.key === this.selectedKey;
  }

  getButtonClass(): string {
    if (this.isSelected()) return 'bg-primary/15 text-primary';
    if (this.isSelectable()) return 'text-foreground hover:bg-secondary';
    return 'text-muted-foreground';
  }

  handleClick(): void {
    if (this.item.type === 'folder') this.expanded.update(v => !v);
    if (this.isSelectable()) this.selected.emit(this.item);
  }

  onChildSelected(item: S3Item): void {
    this.selected.emit(item);
  }
}

@Component({
  selector: 'app-synergy-drive-picker',
  standalone: true,
  imports: [CommonModule, TreeNodeComponent],
  template: `
    <div class="border border-border rounded-lg bg-card overflow-hidden">
      <div class="flex items-center justify-between px-4 py-3 border-b border-border">
        <div>
          <h3 class="text-sm font-semibold">Synergy Drive</h3>
          <p class="text-xs text-muted-foreground">
            {{ mode === 'folder' ? 'Select a destination folder' : 'Select a file' }}
            @if (fileFilter) { <span>({{ fileFilter.join(', ') }})</span> }
          </p>
        </div>
        <button
          (click)="handleRefresh()"
          [disabled]="isLoading()"
          class="h-8 w-8 flex items-center justify-center rounded-md text-muted-foreground hover:text-foreground hover:bg-secondary transition-colors disabled:opacity-50"
        >
          <svg
            class="h-4 w-4"
            [class.animate-spin]="isLoading()"
            xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
          >
            <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/>
            <path d="M21 3v5h-5"/>
            <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/>
            <path d="M8 16H3v5"/>
          </svg>
        </button>
      </div>
      <div class="p-2 max-h-64 overflow-y-auto">
        @for (item of tree; track item.key) {
          <app-tree-node
            [item]="item"
            [depth]="0"
            [mode]="mode"
            [fileFilter]="fileFilter"
            [selectedKey]="selectedKey"
            (selected)="onSelect.emit($event)"
          />
        }
      </div>
    </div>
  `,
})
export class SynergyDrivePickerComponent {
  @Input() mode: 'file' | 'folder' = 'file';
  @Input() fileFilter?: string[];
  @Input() selectedKey?: string;
  @Output() onSelect = new EventEmitter<S3Item>();

  isLoading = signal(false);
  tree = MOCK_TREE;

  handleRefresh(): void {
    this.isLoading.set(true);
    // TODO: Call Synergy Drive API
    setTimeout(() => this.isLoading.set(false), 800);
  }
}
