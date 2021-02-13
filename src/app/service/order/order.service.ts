import { Injectable } from '@angular/core';
import { HttpClientService } from 'src/app/shared/http-client/http-client.service';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class OrderService {
  private baseUrl = environment.baseUrl+'order';
 
  constructor(private httpClientService: HttpClientService) { }

  getOrders(pageNo:number,pageSize:number){
    let url = `${this.baseUrl}?pageNo=${pageNo}&pageSize=${pageSize}`
    return this.httpClientService.get(url);
  }
}
