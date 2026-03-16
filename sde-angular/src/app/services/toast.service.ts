import { Injectable, signal } from '@angular/core';

export interface Toast {
  id: string;
  title: string;
  description?: string;
  variant?: 'default' | 'destructive' | 'success';
}

@Injectable({ providedIn: 'root' })
export class ToastService {
  private _toasts = signal<Toast[]>([]);
  readonly toasts = this._toasts.asReadonly();

  private add(toast: Omit<Toast, 'id'>): void {
    const id = crypto.randomUUID();
    this._toasts.update(t => [...t, { ...toast, id }]);
    setTimeout(() => this.dismiss(id), 5000);
  }

  success(title: string, description?: string): void {
    this.add({ title, description, variant: 'success' });
  }

  error(title: string, description?: string): void {
    this.add({ title, description, variant: 'destructive' });
  }

  info(title: string, description?: string): void {
    this.add({ title, description, variant: 'default' });
  }

  toast(opts: { title: string; description?: string; variant?: 'default' | 'destructive' }): void {
    this.add(opts);
  }

  dismiss(id: string): void {
    this._toasts.update(t => t.filter(x => x.id !== id));
  }
}
