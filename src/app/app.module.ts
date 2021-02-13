import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconModule } from '@angular/material/icon';
import { LayoutComponent } from './components/layout/layout.component';
import { SidemenuComponent } from './components/sidemenu/sidemenu.component';
import { OrderComponent } from './components/order/order.component';
import { ProductComponent } from './components/product/product.component';
import { HeaderComponent } from './components/header/header.component';
import { DashboardComponent } from './components/dashboard/dashboard.component';
<<<<<<< HEAD
import { MatCheckboxModule } from '@angular/material/checkbox';
=======
import { ChartsModule } from 'ng2-charts';
import { FormsModule } from "@angular/forms";
import {MatCheckboxModule} from '@angular/material/checkbox';
>>>>>>> 2869cccd0e0b9cb02a7185c12d55ad3466876b81
import { HttpClientModule } from '@angular/common/http';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import {MatMenuModule} from '@angular/material/menu';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { AddProductComponent } from './components/add-product/add-product.component';
import {MatSlideToggleModule} from '@angular/material/slide-toggle';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';


@NgModule({
  declarations: [
    AppComponent,
    LayoutComponent,
    SidemenuComponent,
    OrderComponent,
    ProductComponent,
    HeaderComponent,
    DashboardComponent,
    AddProductComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    MatIconModule,
    ChartsModule,
    FormsModule,
    MatCheckboxModule,
    HttpClientModule,
    MatProgressBarModule,
    MatMenuModule,
    NgbModule,
    MatSlideToggleModule,
    ReactiveFormsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
