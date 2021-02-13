import { Injectable } from '@angular/core';
import { HttpClientService } from 'src/app/shared/http-client/http-client.service';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class BrandService {
  private baseUrl = environment.baseUrl+'productBrand';

  constructor(private httpClient: HttpClientService) { }

  getBrand(){
    return this.httpClient.get(this.baseUrl); 
  }
}
