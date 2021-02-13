import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ProductService } from 'src/app/service/product/product-service';

@Component({
  selector: 'app-product',
  templateUrl: './product.component.html',
  styleUrls: ['./product.component.scss']
})
export class ProductComponent implements OnInit {

  productList:any;

  constructor(private productService: ProductService,private router: Router) { }


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

  onAddProduct(){
    this.router.navigate(['/add-product'])
    console.log("add Products")
  }

}
