import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Subject, Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class JobsService {
  private apiUrl = 'http://localhost:8000';
  private ws!: WebSocket;
  private wsSubject = new Subject<any>();

  constructor(private http: HttpClient) {
    this.connectWebSocket();
  }

  private connectWebSocket() {
    this.ws = new WebSocket('ws://localhost:8000/ws');
    this.ws.onmessage = (event) => {
      this.wsSubject.next(JSON.parse(event.data));
    };
    this.ws.onclose = () => {
      setTimeout(() => this.connectWebSocket(), 3000); 
    };
  }

  getJobs(status?: string): Observable<any> {
    const params = status ? `?status=${status}` : '';
    return this.http.get(`${this.apiUrl}/jobs${params}`);
  }

  getStats(): Observable<any> {
    return this.http.get(`${this.apiUrl}/stats`);
  }

  getJobDetail(id: string): Observable<any> {
    return this.http.get(`${this.apiUrl}/jobs/${id}`);
  }

  onUpdate(): Observable<any> {
    return this.wsSubject.asObservable();
  }
}