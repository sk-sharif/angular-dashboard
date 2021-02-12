import { Component, OnInit } from '@angular/core';
import { ProductService } from 'src/app/service/product/product-service';

@Component({
  selector: 'app-product',
  templateUrl: './product.component.html',
  styleUrls: ['./product.component.scss']
})
export class ProductComponent implements OnInit {

  productList:any;

  constructor(private productService: ProductService) { }


  ngOnInit(): void {
    this.initializeProduct();
  }

  initializeProduct(){
    this.productService.getProducts(0,100).subscribe(response=>{
      if(response){
        this.productList = response.content;
        console.log("products",response.content);
      }
    })
  }

}
