import { Component, OnInit, OnDestroy, ChangeDetectorRef } from "@angular/core";
import { CommonModule } from "@angular/common";
import { HttpClient, HttpClientModule } from "@angular/common/http";
import { Subscription, interval, of } from "rxjs";
import { switchMap, startWith, catchError } from "rxjs/operators";
@Component({
  selector:"app-monitor", standalone:true, imports:[CommonModule, HttpClientModule],
  template:`
    <div style="padding:20px;font-family:sans-serif;background:#f0f2f5;min-height:100vh;">
      <h1>File Processing Dashboard</h1>
      <div style="display:flex;gap:16px;margin-bottom:24px;">
        <div style="padding:20px;background:white;border-radius:8px;min-width:140px;box-shadow:0 2px 4px rgba(0,0,0,.1);">
          <div style="font-size:13px;color:#888;">Total Archivos</div>
          <div style="font-size:40px;font-weight:bold;">{{getTotalJobs()}}</div>
        </div>
        <div style="padding:20px;background:white;border-radius:8px;min-width:140px;box-shadow:0 2px 4px rgba(0,0,0,.1);">
          <div style="font-size:13px;color:#888;">Completados</div>
          <div style="font-size:40px;font-weight:bold;color:#4CAF50;">{{getCompletedJobs()}}</div>
        </div>
      </div>
      <div style="background:white;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,.1);">
        <table style="width:100%;border-collapse:collapse;">
          <thead>
            <tr style="background:#fafafa;border-bottom:2px solid #eee;">
              <th style="padding:14px;text-align:left;">Archivo</th>
              <th style="padding:14px;text-align:left;">Estado</th>
              <th style="padding:14px;text-align:left;">Tamano</th>
              <th style="padding:14px;text-align:left;">Filas</th>
              <th style="padding:14px;text-align:left;">Recibido</th>
            </tr>
          </thead>
          <tbody>
            <tr *ngFor="let j of jobs" style="border-bottom:1px solid #f0f0f0;">
              <td style="padding:14px;">{{j.filename}}</td>
              <td style="padding:14px;">
                <span [style.background]="getColor(j.status)" style="padding:3px 10px;border-radius:12px;color:white;font-size:12px;">{{j.status}}</span>
              </td>
              <td style="padding:14px;">{{j.size}} B</td>
              <td style="padding:14px;">{{j.rows_processed || "-"}}</td>
              <td style="padding:14px;">{{j.received_at | slice:0:10}}</td>
            </tr>
            <tr *ngIf="jobs.length===0">
              <td colspan="5" style="padding:30px;text-align:center;color:#bbb;">Sin archivos procesados</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>`
})
export class MonitorComponent implements OnInit, OnDestroy {
  jobs:any[]=[];
  stats:any[]=[];
  private subs:Subscription[]=[];
  colors:any={received:"#2196F3",processing:"#FF9800",completed:"#4CAF50",failed:"#F44336"};

  constructor(private http:HttpClient, private cdr:ChangeDetectorRef){}

  ngOnInit(){
    this.subs.push(
      interval(5000).pipe(
        startWith(0),
        switchMap(()=>this.http.get<any>("/api/jobs").pipe(catchError(()=>of({jobs:[]}))))
      ).subscribe((r:any)=>{
        this.jobs = [...(r.jobs||[])];
        this.cdr.detectChanges();
      }),
      interval(10000).pipe(
        startWith(0),
        switchMap(()=>this.http.get<any>("/api/stats").pipe(catchError(()=>of({stats:[]}))))
      ).subscribe((r:any)=>{
        this.stats = [...(r.stats||[])];
        this.cdr.detectChanges();
      })
    );
  }

  ngOnDestroy(){this.subs.forEach(s=>s.unsubscribe());}
  getColor(s:string){return this.colors[s]||"#999";}
  getTotalJobs(){return this.stats.reduce((a,s)=>a+ +s.count,0);}
  getCompletedJobs(){return this.stats.find((s:any)=>s.status==="completed")?.count||0;}
}