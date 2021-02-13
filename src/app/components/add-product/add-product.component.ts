import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { BrandService } from 'src/app/service/brand/brand.service';
import { CategoryService } from 'src/app/service/category/category.service';
import { ProductService } from 'src/app/service/product/product-service';

@Component({
  selector: 'app-add-product',
  templateUrl: './add-product.component.html',
  styleUrls: ['./add-product.component.scss']
})
export class AddProductComponent implements OnInit {
  productForm!: FormGroup;
  category: any;
  subCategory: any;
  subCategoryType: any;
  brand: any;
  submitted: boolean = false;
  files: any;

  constructor(private categoryService: CategoryService,
    private brandService: BrandService,
    private productService: ProductService) { }

  ngOnInit(): void {
    this.initializeForm();
    this.loadCategory();
    this.loadBrand();
  }

  loadBrand() {
    this.brandService.getBrand().subscribe(response => {
      if (response) {
        this.brand = response;
      }
    })
  }

  loadCategory() {
    this.categoryService.getCategory().subscribe(response => {
      if (response) {
        this.category = response;
        console.log("category", response);
      }
    })
  }

  initializeForm() {
    this.productForm = new FormGroup({
      'type': new FormControl(0, Validators.required),
      'category': new FormControl(0, Validators.required),
      'subCategory': new FormControl(0, Validators.required),
      'subCategoryType': new FormControl(0, Validators.required),
      'status': new FormControl(false),
      'featured': new FormControl(false),
      'name': new FormControl('', Validators.required),
      'description': new FormControl('', Validators.required),
      'tag': new FormControl('', Validators.required),
      'manufacture': new FormControl(0, Validators.required),
      'actualPrice': new FormControl('', Validators.required),
      'sellPrice': new FormControl('', Validators.required),
      'quantity': new FormControl('', Validators.required),
      'size': new FormControl(''),
      'color': new FormControl(''),
      'specification': new FormControl('', Validators.required),
    })
  }

  selectCategory() {
    this.subCategory = this.productForm.get('category')?.value.subCategoryList;
  }

  selectSubCategory() {
    this.subCategoryType = this.productForm.get('subCategory')?.value.subCategoryTypeList;
  }

  upload(files: any) {
    if(files){
      this.files = files;
    }
  }

  onSaveProduct() {
    this.submitted = true;
    console.log("formData",this.productForm);
    if (this.productForm.valid && this.files) {
      let formdata = new FormData();
      formdata.append("productName", this.productForm.value.name);
      formdata.append("productDescription", this.productForm.value.description);
      formdata.append("purchasePrice", this.productForm.value.actualPrice);
      formdata.append("salePrice", this.productForm.value.sellPrice);
      formdata.append("totalQuantity", this.productForm.value.quantity);
      formdata.append("vendorId", "1");
      formdata.append("brandId", this.productForm.value.manufacture.brandId);
      formdata.append("categoryId", this.productForm.value.category.categoryId);
      formdata.append("subcategoryId", this.productForm.value.subCategory.subCategoryId);
      formdata.append("subCategoryTypeId", this.productForm.value.subCategoryType.subCategoryTypeId);
      formdata.append("productSpecification", this.productForm.value.specification);
      if (this.productForm.value.size) {
        formdata.append("size", this.productForm.value.size);
      }
      if (this.productForm.value.color) {
        formdata.append("colorName", this.productForm.value.color);
      }

      for (let i = 0; i < this.files.length; i++) {
        const file = this.files[i];
        formdata.append("files", file);
      }
      this.productService.saveProduct(formdata).subscribe(response=>{
        console.log("response",response);
      });

    }
  }

}
