import { Injectable } from '@angular/core';
import { HttpClientService } from 'src/app/shared/http-client/http-client.service';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class CategoryService {

  constructor(private httpClient: HttpClientService) { }

  private baseUrl = environment.baseUrl+'category';

  getCategory(){
    return this.httpClient.get(this.baseUrl);
  }

}
