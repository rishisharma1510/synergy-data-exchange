import { Injectable, signal, computed } from '@angular/core';

export interface User {
  id: string;
  email: string;
  name: string;
  tenantId: string;
  tenantName: string;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private _user = signal<User>({
    id: 'usr-001',
    email: 'user@example.com',
    name: 'Rishi Doe',
    tenantId: 'tenant-acme',
    tenantName: 'DataBridge',
  });

  readonly user = this._user.asReadonly();
  readonly isAuthenticated = computed(() => !!this._user());
  readonly isLoading = signal(false);

  login(): void {
    // TODO: Integrate with Okta SPA SDK
    console.log('Okta login triggered');
  }

  logout(): void {
    // TODO: Integrate with Okta SPA SDK
    console.log('Okta logout triggered');
  }
}
