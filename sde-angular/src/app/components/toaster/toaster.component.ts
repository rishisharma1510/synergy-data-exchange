import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ToastService } from '../../services/toast.service';

@Component({
  selector: 'app-toaster',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm w-full">
      @for (toast of toastService.toasts(); track toast.id) {
        <div
          class="flex items-start gap-3 rounded-lg border p-4 shadow-lg text-sm transition-all"
          [class]="getToastClass(toast.variant)"
        >
          <div class="flex-1 min-w-0">
            <p class="font-semibold">{{ toast.title }}</p>
            @if (toast.description) {
              <p class="text-xs mt-1 opacity-80">{{ toast.description }}</p>
            }
          </div>
          <button
            (click)="toastService.dismiss(toast.id)"
            class="shrink-0 opacity-60 hover:opacity-100 transition-opacity"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M18 6 6 18"/><path d="m6 6 12 12"/>
            </svg>
          </button>
        </div>
      }
    </div>
  `,
})
export class ToasterComponent {
  readonly toastService = inject(ToastService);

  getToastClass(variant?: string): string {
    switch (variant) {
      case 'destructive':
        return 'bg-destructive text-destructive-foreground border-destructive/30';
      case 'success':
        return 'bg-card text-foreground border-success/30';
      default:
        return 'bg-card text-foreground border-border';
    }
  }
}
