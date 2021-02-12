import { Injectable } from '@angular/core';
import { HttpClientService } from 'src/app/shared/http-client/http-client.service';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class ProductService {

  private baseUrl = environment.baseUrl+'product';
  
  constructor(private httpClientService: HttpClientService) { }

  getProducts(pageNo:number,pageSize:number){
    let url = `${this.baseUrl}?pageNo=${pageNo}&pageSize=${pageSize}`
    return this.httpClientService.get(url);
  }
}
